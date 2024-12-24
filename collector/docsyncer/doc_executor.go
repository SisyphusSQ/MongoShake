package docsyncer

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alibaba/MongoShake/v2/lib/retry"
	"hash/crc32"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/lib/log"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/tunnel/kafka"
)

var (
	GlobalCollExecutorId int32 = -1
	GlobalDocExecutorId  int32 = -1
)

func GenerateDocExecutorId() int {
	return int(atomic.AddInt32(&GlobalDocExecutorId, 1))
}

type CollectionExecutor struct {
	// multi executor
	executors []RowExecutor

	// worker id
	id int

	// mongo url
	url         string
	sslRootFile string

	ns utils.NS

	wg sync.WaitGroup
	// batchCount int64

	conn *utils.MongoCommunityConn

	docBatch chan []*bson.Raw

	// not own
	syncer *DBSyncer
}

func GenerateCollExecutorId() int {
	return int(atomic.AddInt32(&GlobalCollExecutorId, 1))
}

func NewCollectionExecutor(id int, url string, ns utils.NS, syncer *DBSyncer, sslRootFile string) *CollectionExecutor {
	return &CollectionExecutor{
		id:          id,
		url:         url,
		sslRootFile: sslRootFile,
		ns:          ns,
		syncer:      syncer,
		// batchCount: 0,
	}
}

func (colExecutor *CollectionExecutor) Start() error {
	var err error
	if !conf.Options.FullSyncExecutorDebug && !conf.Options.FullSyncKafkaSend {
		writeConcern := utils.ReadWriteConcernDefault
		if conf.Options.FullSyncExecutorMajorityEnable {
			writeConcern = utils.ReadWriteConcernMajority
		}
		if colExecutor.conn, err = utils.NewMongoCommunityConn(colExecutor.url,
			utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernDefault, writeConcern,
			colExecutor.sslRootFile); err != nil {
			return err
		}
	}

	parallel := conf.Options.FullSyncReaderWriteDocumentParallel
	colExecutor.docBatch = make(chan []*bson.Raw, parallel)

	executors := make([]RowExecutor, parallel)
	for i := 0; i != len(executors); i++ {
		// Client is a handle representing a pool of connections, can be used by multi routines
		// You Can get one idle connection, if all is idle, then always get the same one
		// connections pool default parameter(min_conn:0 max_conn:100 create_conn_once:2)
		if conf.Options.FullSyncKafkaSend {
			executors[i], err = NewMsgExecutor(GenerateCollExecutorId(), colExecutor, colExecutor.url, colExecutor.syncer)
			if err != nil {
				l.Logger.Errorf("generate kafka executor failed, %v", err)
				return err
			}
		} else {
			executors[i] = NewDocExecutor(GenerateDocExecutorId(), colExecutor, colExecutor.conn, colExecutor.syncer)
		}
		go executors[i].start()
	}
	colExecutor.executors = executors
	return nil
}

func (colExecutor *CollectionExecutor) Sync(docs []*bson.Raw) {
	count := uint64(len(docs))
	if count == 0 {
		return
	}

	/*
	 * TODO, waitGroup.Add may overflow, so use atomic to replace waitGroup
	 * // colExecutor.wg.Add(1)
	 */
	colExecutor.wg.Add(1)
	// atomic.AddInt64(&colExecutor.batchCount, 1)
	colExecutor.docBatch <- docs
}

func (colExecutor *CollectionExecutor) Wait() error {
	colExecutor.wg.Wait()
	/*for v := atomic.LoadInt64(&colExecutor.batchCount); v != 0; {
		utils.YieldInMs(1000)
		l.Logger.Infof("CollectionExecutor[%v %v] wait batchCount[%v] == 0", colExecutor.ns, colExecutor.id, v)
	}*/

	close(colExecutor.docBatch)
	if !conf.Options.FullSyncExecutorDebug && !conf.Options.FullSyncKafkaSend {
		colExecutor.conn.Close()
	}

	for _, exec := range colExecutor.executors {
		if exec.Error() != nil {
			return errors.New(fmt.Sprintf("sync ns %v failed. %v", colExecutor.ns, exec.Error()))
		}
	}
	return nil
}

type RowExecutor interface {
	String() string

	start()

	Error() error
}

type DocExecutor struct {
	// sequence index id in each replayer
	id int
	// colExecutor, not owned
	colExecutor *CollectionExecutor

	conn *utils.MongoCommunityConn

	error error

	// not own
	syncer *DBSyncer
}

func NewDocExecutor(id int, colExecutor *CollectionExecutor, conn *utils.MongoCommunityConn,
	syncer *DBSyncer) *DocExecutor {
	return &DocExecutor{
		id:          id,
		colExecutor: colExecutor,
		conn:        conn,
		syncer:      syncer,
	}
}

func (exec *DocExecutor) String() string {
	return fmt.Sprintf("DocExecutor[%v] collectionExecutor[%v]", exec.id, exec.colExecutor.ns)
}

func (exec *DocExecutor) Error() error {
	return exec.error
}

func (exec *DocExecutor) start() {
	if !conf.Options.FullSyncExecutorDebug {
		defer exec.conn.Close()
	}

	for {
		docs, ok := <-exec.colExecutor.docBatch
		if !ok {
			break
		}

		if exec.error == nil {
			if err := exec.doSync(docs); err != nil {
				exec.error = err
				// since v2.4.11: panic directly if meets error
				l.Logger.Panicf("%s sync failed: %v", exec, err)
			}
		}

		exec.colExecutor.wg.Done()
		// atomic.AddInt64(&exec.colExecutor.batchCount, -1)
	}
}

// use by full sync
func (exec *DocExecutor) doSync(docs []*bson.Raw) error {
	if len(docs) == 0 || conf.Options.FullSyncExecutorDebug {
		return nil
	}

	ns := exec.colExecutor.ns

	var models []mongo.WriteModel
	for _, doc := range docs {

		if conf.Options.FullSyncExecutorFilterOrphanDocument && exec.syncer.orphanFilter != nil {
			var docData bson.D
			if err := bson.Unmarshal(*doc, &docData); err != nil {
				l.Logger.Errorf("doSync do bson unmarshal %v failed. %v", doc, err)
				continue
			}
			// judge whether is orphan document, pass if so
			if exec.syncer.orphanFilter.Filter(docData, ns.Database+"."+ns.Collection) {
				l.Logger.Infof("orphan document [%v] filter", doc)
				continue
			}
		}

		models = append(models, mongo.NewInsertOneModel().SetDocument(doc))
	}

	// qps limit if enable
	if exec.syncer.qos != nil && exec.syncer.qos.Limit > 0 {
		exec.syncer.qos.FetchBucket()
	}

	/*
		if conf.Options.LogLevel == utils.VarLogLevelDebug {
			var docBeg, docEnd bson.M
			bson.Unmarshal(*docs[0], &docBeg)
			bson.Unmarshal(*docs[len(docs)-1], &docEnd)
			l.Logger.Debugf("DBSyncer id[%v] doSync BulkWrite with table[%v] batch _id interval [%v, %v]", exec.syncer.id, ns,
				docBeg, docEnd)
		}
	*/

	opts := options.BulkWrite().SetOrdered(false)
	res, err := exec.conn.Client.Database(ns.Database).Collection(ns.Collection).BulkWrite(nil, models, opts)

	if err != nil {
		if _, ok := err.(mongo.BulkWriteException); !ok {
			return fmt.Errorf("bulk run failed[%v]", err)
		}

		l.Logger.Warnf("insert docs with length[%v] into ns[%v] of dest mongo failed[%v] res[%v]",
			len(models), ns, (err.(mongo.BulkWriteException)).WriteErrors[0], res)

		var updateModels []mongo.WriteModel
		for _, wError := range (err.(mongo.BulkWriteException)).WriteErrors {
			if utils.DuplicateKey(wError) {
				if !conf.Options.FullSyncExecutorInsertOnDupUpdate {
					return fmt.Errorf("duplicate key error[%v], you can clean the document on the target mongodb, "+
						"or enable %v to solve, but full-sync stage needs restart",
						wError, "full_sync.executor.insert_on_dup_update")
				}

				dupDocument := *docs[wError.Index]
				var updateFilter bson.D
				updateFilterBool := false
				var docData bson.D
				if err := bson.Unmarshal(dupDocument, &docData); err == nil {
					for _, bsonE := range docData {
						if bsonE.Key == "_id" {
							updateFilter = bson.D{bsonE}
							updateFilterBool = true
						}
					}
				}
				if updateFilterBool == false {
					return fmt.Errorf("duplicate key error[%v], can't get _id from document", wError)
				}
				updateModels = append(updateModels, mongo.NewUpdateOneModel().
					SetFilter(updateFilter).SetUpdate(bson.D{{"$set", dupDocument}}))
			} else {
				return fmt.Errorf("bulk run failed[%v]", wError)
			}
		}

		if len(updateModels) != 0 {
			opts := options.BulkWrite().SetOrdered(false)
			_, err := exec.conn.Client.Database(ns.Database).Collection(ns.Collection).BulkWrite(nil, updateModels, opts)
			if err != nil {
				return fmt.Errorf("bulk run updateForInsert failed[%v]", err)
			}
			l.Logger.Debugf("updateForInsert succ updateModels.len:%d updateModules[0]:%v\n",
				len(updateModels), updateModels[0])
		} else {
			return fmt.Errorf("bulk run failed[%v]", err)
		}
	}

	return nil
}

type MsgExecutor struct {
	// sequence index id in each replayer
	id int

	// colExecutor, not owned
	colExecutor *CollectionExecutor

	error error

	asyncer *kafka.AsyncWriter

	// not own
	syncer *DBSyncer
}

func NewMsgExecutor(id int, colExecutor *CollectionExecutor, kafkaUrl string, syncer *DBSyncer) (*MsgExecutor, error) {
	var err error
	m := &MsgExecutor{
		id:          id,
		colExecutor: colExecutor,
		syncer:      syncer,
	}

	//m.asyncer, err = kafka.NewSyncWriter(conf.Options.TunnelMongoSslRootCaFile, kafkaUrl, 1)
	m.asyncer, err = kafka.NewAsyncWriterWithFull(conf.Options.TunnelMongoSslRootCaFile, kafkaUrl)
	//m.asyncer, err = ka.NewAsyncWriter(conf.Options.TunnelMongoSslRootCaFile, kafkaUrl)
	if err != nil {
		return nil, err
	}
	//_ = m.asyncer.Start()

	return m, nil
}

func (exec *MsgExecutor) String() string {
	return fmt.Sprintf("MsgExecutor[%v] collectionExecutor[%v]", exec.id, exec.colExecutor.ns.Str())
}

func (exec *MsgExecutor) start() {
	for {
		docs, ok := <-exec.colExecutor.docBatch
		if !ok {
			break
		}

		if exec.error == nil {
			if err := exec.doSync(docs); err != nil {
				exec.error = err
				// panic directly if meets error
				l.Logger.Panicf("%s sync failed: %v", exec, err)
			}
		}

		exec.colExecutor.wg.Done()
	}
}

func (exec *MsgExecutor) doSync(docs []*bson.Raw) error {
	if len(docs) == 0 || conf.Options.FullSyncExecutorDebug {
		return nil
	}

	var (
		ns  = exec.colExecutor.ns.Str()
		pid int32
		//pid = int32(int(crc32.ChecksumIEEE([]byte(ns))) % conf.Options.TunnelKafkaPartitionNumber)
		//pid = rand.Int32N(9)
		//total = conf.Options.TunnelKafkaPartitionNumber
	)

	if conf.Options.FullSyncReaderParallelIndex == "_id" {
		b := docs[0].Lookup("_id")
		switch b.Type {
		case bson.TypeObjectID:
			v := b.ObjectID()
			pid = int32(int(crc32.ChecksumIEEE([]byte(v.Hex()))) % conf.Options.TunnelKafkaPartitionNumber)
		case bson.TypeString:
			v := b.String()
			pid = int32(int(crc32.ChecksumIEEE([]byte(v))) % conf.Options.TunnelKafkaPartitionNumber)
		default:
			pid = rand.Int32N(int32(conf.Options.TunnelKafkaPartitionNumber))
		}
	} else {
		pid = int32(int(crc32.ChecksumIEEE([]byte(ns))) % conf.Options.TunnelKafkaPartitionNumber)
	}

	for _, doc := range docs {
		if conf.Options.FullSyncExecutorFilterOrphanDocument && exec.syncer.orphanFilter != nil {
			var docData bson.D
			if err := bson.Unmarshal(*doc, &docData); err != nil {
				l.Logger.Errorf("doSync do bson unmarshal %v failed. %v", doc, err)
				continue
			}
			// judge whether is orphan document, pass if so
			if exec.syncer.orphanFilter.Filter(docData, ns) {
				l.Logger.Infof("orphan document [%v] filter", doc)
				continue
			}
		}

		var (
			err     error
			encode  []byte
			docData bson.M
			ev      = oplog.CanalEvent{
				Timestamp: time.Now().UnixMilli(),
				Namespace: ns,
				EventType: "INSERT",
				RAG:       conf.Options.RAG,
			}
		)

		if err = bson.Unmarshal(*doc, &docData); err != nil {
			l.Logger.Errorf("doSync do bson unmarshal %v failed. %v", doc, err)
			continue
		}
		ev.RowAfter = docData

		// -------- calculate partition id --------
		//if id, ok := docData["_id"]; ok {
		//	pid = int32(int(crc32.ChecksumIEEE([]byte(id.(primitive.ObjectID).Hex()))) % total)
		//}

		if encode, err = json.Marshal(ev); err != nil {
			l.Logger.Errorf("ev do json unmarshal %v failed. %v", ev, err)
			continue
		}

		err = retry.Do(func() error {
			err = exec.asyncer.Send(encode, pid)
			if err != nil {
				l.Logger.Errorf("doSync async send [%s] failed, msg will retry. err: %v", string(encode), err)
				return err
			}

			return nil
		}, 10, 2*time.Second)
		if err != nil {
			l.Logger.Errorf("doSync async send [%s] failed. err: %v", string(encode), err)
			return err
		}
	}

	// qps limit if enable
	if exec.syncer.qos != nil && exec.syncer.qos.Limit > 0 {
		exec.syncer.qos.FetchBucket()
	}

	/*
		if conf.Options.LogLevel == utils.VarLogLevelDebug {
			var docBeg, docEnd bson.M
			bson.Unmarshal(*docs[0], &docBeg)
			bson.Unmarshal(*docs[len(docs)-1], &docEnd)
			l.Logger.Debugf("DBSyncer id[%v] doSync BulkWrite with table[%v] batch _id interval [%v, %v]", exec.syncer.id, ns,
				docBeg, docEnd)
		}
	*/

	return nil
}

func (exec *MsgExecutor) Error() error {
	return exec.error
}
