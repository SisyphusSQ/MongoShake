package collector

// persist oplog on disk

import (
	"sync"
	"sync/atomic"
	"time"

	diskQueue "github.com/SisyphusSQ/go-diskqueue"
	nimo "github.com/gugemichael/nimo4go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/lib/log"
	"github.com/alibaba/MongoShake/v2/oplog"
)

const (
	FullSyncReaderOplogStoreDiskReadBatch = 10000
)

type Persister struct {
	replset string       // name
	sync    *OplogSyncer // not owned, inner call

	// batch data([]byte) together and send to downstream
	Buffer            [][]byte
	nextQueuePosition uint64

	// enable disk persist
	enableDiskPersist bool

	// stage of fetch and store oplog
	fetchStage int32
	// disk queue used to store oplog temporarily
	DiskQueue       *diskQueue.DiskQueue
	diskQueueMutex  sync.Mutex // disk queue mutex
	diskQueueLastTs int64      // the last oplog timestamp in disk queue(full timestamp, have T + I)

	// metric info, used in print
	diskWriteCount uint64
	diskReadCount  uint64
}

func NewPersister(replset string, sync *OplogSyncer) *Persister {
	p := &Persister{
		replset:           replset,
		sync:              sync,
		Buffer:            make([][]byte, 0, conf.Options.IncrSyncFetcherBufferCapacity),
		nextQueuePosition: 0,
		enableDiskPersist: conf.Options.SyncMode == utils.VarSyncModeAll &&
			conf.Options.FullSyncReaderOplogStoreDisk,
		fetchStage:      utils.FetchStageStoreUnknown,
		diskQueueLastTs: -1, // initial set 1
	}

	return p
}

func (p *Persister) Start() {
	if p.enableDiskPersist {
		go p.retrieve()
	}
}

func (p *Persister) SetFetchStage(fetchStage int32) {
	l.Logger.Infof("persister replset[%v] update fetch status to: %v", p.replset, utils.LogFetchStage(fetchStage))
	atomic.StoreInt32(&p.fetchStage, fetchStage)
}

func (p *Persister) GetFetchStage() int32 {
	return atomic.LoadInt32(&p.fetchStage)
}

func (p *Persister) InitDiskQueue(dqName string) {
	fetchStage := p.GetFetchStage()
	// fetchStage shouldn't change between here
	if fetchStage != utils.FetchStageStoreDiskNoApply && fetchStage != utils.FetchStageStoreDiskApply {
		l.Logger.Panicf("persister replset[%v] init disk queue in illegal fetchStage %v",
			p.replset, utils.LogFetchStage(fetchStage))
	}
	if p.DiskQueue != nil {
		l.Logger.Panicf("init disk queue failed: already exist")
	}

	/*
		p.DiskQueue = diskQueue.NewDiskQueue(dqName, conf.Options.LogDirectory,
			conf.Options.FullSyncReaderOplogStoreDiskMaxSize, FullSyncReaderOplogStoreDiskReadBatch,
			1<<30, 0, 1<<26,
			1000, 2*time.Second)
	*/
	p.DiskQueue = diskQueue.NewDiskQueueWithLogger(dqName, conf.Options.LogDirectory,
		conf.Options.FullSyncReaderOplogStoreDiskMaxSize, FullSyncReaderOplogStoreDiskReadBatch,
		1<<30, 0, 1<<26,
		1000, 2*time.Second,
		diskQueue.VerbosePrintfLogger(l.Logger))
}

func (p *Persister) GetQueryTsFromDiskQueue() primitive.Timestamp {
	if p.DiskQueue == nil {
		l.Logger.Panicf("persister replset[%v] get query timestamp from nil disk queue", p.replset)
	}

	logData := p.DiskQueue.GetLastWriteData()
	if len(logData) == 0 {
		return primitive.Timestamp{}
	}

	if conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodOplog {
		log := new(oplog.ParsedLog)
		if err := bson.Unmarshal(logData, log); err != nil {
			l.Logger.Panicf("unmarshal oplog[%v] failed[%v]", logData, err)
		}

		// assert
		if log.Namespace == "" && log.Operation != "n" {
			l.Logger.Panicf("unmarshal data to oplog failed: %v", log)
		}
		return log.Timestamp
	} else {
		// change_stream
		log := new(oplog.Event)
		if err := bson.Unmarshal(logData, log); err != nil {
			l.Logger.Panicf("unmarshal oplog[%v] failed[%v]", logData, err)
		}

		// assert
		if log.OperationType == "" {
			l.Logger.Panicf("unmarshal data to change stream event failed: %v", log)
		}
		return log.ClusterTime
	}
}

// Inject data
func (p *Persister) Inject(input []byte) {
	// only used to test the reader, discard anything
	switch conf.Options.IncrSyncReaderDebug {
	case utils.VarIncrSyncReaderDebugNone:
		// do nothing
	case utils.VarIncrSyncReaderDebugDiscard:
		return
	case utils.VarIncrSyncReaderDebugPrint:
		var test interface{}
		bson.Unmarshal(input, &test)
		l.Logger.Info("print debug: %v", test)
	default:
		// do nothing
	}

	if p.enableDiskPersist {
		// current fetch stage
		fetchStage := p.GetFetchStage()
		if fetchStage == utils.FetchStageStoreMemoryApply {
			p.PushToPendingQueue(input)
		} else if p.DiskQueue != nil {
			if input == nil {
				// no need to store
				return
			}

			// store local
			p.diskQueueMutex.Lock()
			defer p.diskQueueMutex.Unlock()
			if p.DiskQueue != nil {
				// double check, if full and disk apply is finish, dishQueue should be nil
				atomic.AddUint64(&p.diskWriteCount, 1)
				if err := p.DiskQueue.Put(input); err != nil {
					l.Logger.Panicf("persister inject replset[%v] put oplog to disk queue failed[%v]",
						p.replset, err)
				}
			} else {
				// should send to pending queue
				p.PushToPendingQueue(input)
			}
		} else {
			l.Logger.Panicf("persister inject replset[%v] has no diskQueue with fetch stage[%v]",
				p.replset, utils.LogFetchStage(fetchStage))
		}
	} else {
		p.PushToPendingQueue(input)
	}
}

func (p *Persister) PushToPendingQueue(input []byte) {
	flush := false
	if input != nil {
		p.Buffer = append(p.Buffer, input)
	} else {
		flush = true
	}

	if len(p.Buffer) >= conf.Options.IncrSyncFetcherBufferCapacity || (flush && len(p.Buffer) != 0) {
		// we could simply ++syncer.resolverIndex. The max uint64 is 9223372036854774807
		// and discard the skip situation. we assume nextQueueCursor couldn't be overflow
		selected := int(p.nextQueuePosition % uint64(len(p.sync.PendingQueue)))
		p.sync.PendingQueue[selected] <- p.Buffer

		// clear old Buffer, we shouldn't use "p.Buffer = p.Buffer[:0]" because these address won't
		// be changed in the channel.
		// p.Buffer = p.Buffer[:0]
		p.Buffer = make([][]byte, 0, conf.Options.IncrSyncFetcherBufferCapacity)

		// queue position = (queue position + 1) % n
		p.nextQueuePosition++
	}
}

func (p *Persister) retrieve() {
	interval := time.NewTicker(3 * time.Second)
	defer interval.Stop()
	for range interval.C {
		stage := atomic.LoadInt32(&p.fetchStage)
		switch stage {
		case utils.FetchStageStoreDiskApply:
			goto apply
		case utils.FetchStageStoreUnknown, utils.FetchStageStoreDiskNoApply:
			// do nothing
		case utils.FetchStageStoreMemoryApply:
			l.Logger.Info("stage[%v] no need to apply data", utils.LogFetchStage(stage))
			return
		default:
			l.Logger.Panicf("invalid fetch stage[%v]", utils.LogFetchStage(stage))
		}
	}

apply:
	l.Logger.Info("persister retrieve for replset[%v] begin to read from disk queue with depth[%v]",
		p.replset, p.DiskQueue.Depth())
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case readData := <-p.DiskQueue.ReadChan():
			if len(readData) == 0 {
				continue
			}

			atomic.AddUint64(&p.diskReadCount, uint64(len(readData)))
			for _, data := range readData {
				p.PushToPendingQueue(data)
			}

			// move to next read
			if err := p.DiskQueue.Next(); err != nil {
				l.Logger.Panicf("persister replset[%v] retrieve get next failed[%v]", p.replset, err)
			}
		case <-ticker.C:
			// check no more data batching?
			if p.DiskQueue.Depth() < p.DiskQueue.BatchCount() {
				goto finish
			}
		}
	}

finish:
	l.Logger.Info("persister retrieve for replset[%v] block fetch with disk queue depth[%v]",
		p.replset, p.DiskQueue.Depth())

	// wait to finish retrieve and continue fetch to store to memory
	p.diskQueueMutex.Lock()
	defer p.diskQueueMutex.Unlock() // lock till the end
	readData := p.DiskQueue.ReadAll()
	if len(readData) > 0 {
		atomic.AddUint64(&p.diskReadCount, uint64(len(readData)))
		for _, data := range readData {
			// or.oplogChan <- &retOplog{&bson.Raw{Kind: 3, Data: data}, nil}
			p.PushToPendingQueue(data)
		}

		// parse the last oplog timestamp
		p.diskQueueLastTs = utils.TimeStampToInt64(p.GetQueryTsFromDiskQueue())

		if err := p.DiskQueue.Next(); err != nil {
			l.Logger.Panic(err)
		}
	}

	if p.DiskQueue.Depth() != 0 {
		l.Logger.Panicf("persister retrieve for replset[%v] finish, but disk queue depth[%v] is not empty",
			p.replset, p.DiskQueue.Depth())
	}

	p.SetFetchStage(utils.FetchStageStoreMemoryApply)
	if err := p.DiskQueue.Delete(); err != nil {
		l.Logger.Errorf("persister retrieve for replset[%v] close disk queue error. %v", p.replset, err)
	}
	// clean p.DiskQueue
	p.DiskQueue = nil

	l.Logger.Info("persister retriever for replset[%v] exits", p.replset)
}

const opOnDisk = "oplog_on_disk"

func (p *Persister) RestAPI() {
	type PersistNode struct {
		BufferUsed        int    `json:"buffer_used"`
		BufferSize        int    `json:"buffer_size"`
		EnableDiskPersist bool   `json:"enable_disk_persist"`
		FetchStage        string `json:"fetch_stage"`
		DiskWriteCount    uint64 `json:"disk_write_count"`
		DiskReadCount     uint64 `json:"disk_read_count"`
	}

	go func() {
		var enable float64 = 0
		if p.enableDiskPersist {
			enable = 1
		}

		capacity := float64(conf.Options.IncrSyncFetcherBufferCapacity)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			utils.DiskEnableDiskPersist.WithLabelValues(opOnDisk).Set(enable)
			if p.enableDiskPersist {
				utils.DiskBufferSize.WithLabelValues(opOnDisk).Set(capacity)
				utils.DiskBufferUsed.WithLabelValues(opOnDisk).Set(float64(len(p.Buffer)))
				utils.DiskFetchStage.WithLabelValues(opOnDisk, utils.LogFetchStage(p.GetFetchStage())).Set(float64(time.Now().UnixMilli()))
				utils.DiskWriteCount.WithLabelValues(opOnDisk).Add(float64(p.diskWriteCount))
				utils.DiskReadCount.WithLabelValues(opOnDisk).Add(float64(p.diskReadCount))
			}
		}
	}()

	utils.IncrSyncHttpApi.RegisterAPI("/persist", nimo.HttpGet, func([]byte) interface{} {
		return &PersistNode{
			BufferSize:        conf.Options.IncrSyncFetcherBufferCapacity,
			BufferUsed:        len(p.Buffer),
			EnableDiskPersist: p.enableDiskPersist,
			FetchStage:        utils.LogFetchStage(p.GetFetchStage()),
			DiskWriteCount:    atomic.LoadUint64(&p.diskWriteCount),
			DiskReadCount:     atomic.LoadUint64(&p.diskReadCount),
		}
	})
}
