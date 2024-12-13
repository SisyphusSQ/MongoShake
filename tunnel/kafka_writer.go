package tunnel

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/lib/log"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/tunnel/kafka"
)

const (
	inputChanSize  = 256
	outputChanSize = 4196
)

var (
	unitTestWriteKafkaFlag = false
	unitTestWriteKafkaChan chan []byte
)

type outputLog struct {
	ns    string
	isEnd bool
	log   []byte
}

type KafkaWriter struct {
	RemoteAddr   string
	MaxPartition int
	PartitionId  int                // write to which partition
	writer       *kafka.AsyncWriter // writer
	state        int64              // state: ok, error
	encoderNr    int64              // how many encoder
	inputChan    []chan *WMessage   // each encoder has 1 inputChan
	outputChan   []chan outputLog   // output chan length
	pushIdx      int64              // push into which encoder
	popIdx       int64              // pop from which encoder
}

func (tunnel *KafkaWriter) Name() string {
	return "kafka"
}

func (tunnel *KafkaWriter) Prepare() bool {
	var (
		err    error
		writer *kafka.AsyncWriter
	)

	if !unitTestWriteKafkaFlag && conf.Options.IncrSyncTunnelKafkaDebug == "" {
		writer, err = kafka.NewAsyncWriter(conf.Options.TunnelMongoSslRootCaFile, tunnel.RemoteAddr)
		if err != nil {
			l.Logger.Errorf("KafkaWriter prepare[%v] create writer error[%v]", tunnel.RemoteAddr, err)
			return false
		}
		if err = writer.Start(); err != nil {
			l.Logger.Errorf("KafkaWriter prepare[%v] start writer error[%v]", tunnel.RemoteAddr, err)
			return false
		}
	}

	tunnel.writer = writer
	tunnel.state = ReplyOK
	tunnel.encoderNr = int64(math.Max(float64(conf.Options.IncrSyncTunnelWriteThread/conf.Options.IncrSyncWorker), 1))
	tunnel.inputChan = make([]chan *WMessage, tunnel.encoderNr)
	tunnel.outputChan = make([]chan outputLog, tunnel.encoderNr)
	tunnel.pushIdx = 0
	tunnel.popIdx = 0

	l.Logger.Infof("%s starts: writer_thread count[%v]", tunnel, tunnel.encoderNr)

	// start encoder
	for i := 0; i < int(tunnel.encoderNr); i++ {
		tunnel.inputChan[i] = make(chan *WMessage, inputChanSize)
		tunnel.outputChan[i] = make(chan outputLog, outputChanSize)
		go tunnel.encode(i)
	}

	// start kafkaWriter
	go tunnel.writeKafka()

	return true
}

func (tunnel *KafkaWriter) Send(message *WMessage) int64 {
	if len(message.RawLogs) == 0 || message.Tag&MsgProbe != 0 {
		return 0
	}

	encoderId := atomic.AddInt64(&tunnel.pushIdx, 1)
	tunnel.inputChan[encoderId%tunnel.encoderNr] <- message

	// for transfer() not into default branch and then endless loop
	return 0
}

// AckRequired is always false, return 0 directly
func (tunnel *KafkaWriter) AckRequired() bool {
	return false
}

func (tunnel *KafkaWriter) ParsedLogsRequired() bool {
	return false
}

func (tunnel *KafkaWriter) String() string {
	return fmt.Sprintf("KafkaWriter[%v] with partitionId[%v]", tunnel.RemoteAddr, tunnel.PartitionId)
}

func (tunnel *KafkaWriter) encode(id int) {
	for message := range tunnel.inputChan[id] {
		message.Tag |= MsgPersistent
		ns := message.ParsedLogs[0].Namespace

		switch conf.Options.TunnelMessage {
		case utils.VarTunnelMessageBson:
			// write the raw oplog directly
			for i, log := range message.RawLogs {
				tunnel.outputChan[id] <- outputLog{
					ns:    ns,
					isEnd: i == len(log)-1,
					log:   log,
				}
			}

		case utils.VarTunnelMessageCanalJson:
			for i, log := range message.ParsedLogs {
				var (
					err    error
					encode []byte
				)

				ev := oplog.CanalEvent{
					Timestamp: int64(log.Timestamp.T) * 1000,
					Namespace: log.Namespace,
					RAG:       conf.Options.RAG,
				}

				switch log.Operation {
				case "i":
					ev.EventType = "INSERT"
				case "u":
					ev.EventType = "UPDATE"
				case "d":
					ev.EventType = "DELETE"
				default:
					l.Logger.Debugf("this operation[%s] didn't support", log.Operation)
					continue
				}

				if ev.EventType == "DELETE" {
					// 一般来说，应该只有 "_id"
					ev.RowBefore = log.Object.Map()
				} else {
					// insert or update
					if ev.EventType == "UPDATE" {
						ev.PK = log.Query.Map()
					}

					ev.RowAfter = log.Object.Map()

					// transform bson.D to bson.M
					for _, k := range []string{"$set", "$unset"} {
						if res := transformD2M(ev.RowAfter, k); res != nil {
							ev.RowAfter[k] = res
						}
					}
				}

				encode, err = json.Marshal(ev)
				if err != nil {
					if strings.Contains(err.Error(), "unsupported value:") {
						l.Logger.Error("%s json marshal data[%v] meets unsupported value[%v], skip current oplog",
							tunnel, log.ParsedLog, err)
						continue
					} else {
						// should panic
						l.Logger.Panicf("%s json marshal data[%v] error[%v]", tunnel, log.ParsedLog, err)
						tunnel.state = ReplyServerFault
					}
				}

				tunnel.outputChan[id] <- outputLog{
					ns:    ns,
					isEnd: i == len(message.ParsedLogs)-1,
					log:   encode,
				}
			}

		case utils.VarTunnelMessageJson:
			for i, log := range message.ParsedLogs {
				// json marshal
				var encode []byte
				var err error
				if conf.Options.TunnelJsonFormat == "" {
					encode, err = json.Marshal(log.ParsedLog)
					if err != nil {
						if strings.Contains(err.Error(), "unsupported value:") {
							l.Logger.Errorf("%s json marshal data[%v] meets unsupported value[%v], skip current oplog",
								tunnel, log.ParsedLog, err)
							continue
						} else {
							// should panic
							l.Logger.Panicf("%s json marshal data[%v] error[%v]", tunnel, log.ParsedLog, err)
							tunnel.state = ReplyServerFault
						}
					}
				} else if conf.Options.TunnelJsonFormat == "canonical_extended_json" {
					encode, err = bson.MarshalExtJSON(log.ParsedLog, true, true)
					if err != nil {
						// should panic
						l.Logger.Panicf("%s json marshal data[%v] error[%v]", tunnel, log.ParsedLog, err)
						tunnel.state = ReplyServerFault
					}
				} else {
					l.Logger.Panicf("unknown tunnel.json.format[%v]", conf.Options.TunnelJsonFormat)
				}

				tunnel.outputChan[id] <- outputLog{
					ns:    ns,
					isEnd: i == len(message.ParsedLogs)-1,
					log:   encode,
				}
			}

		case utils.VarTunnelMessageRaw:
			byteBuffer := bytes.NewBuffer([]byte{})
			// checksum
			binary.Write(byteBuffer, binary.BigEndian, uint32(message.Checksum))
			// tag
			binary.Write(byteBuffer, binary.BigEndian, uint32(message.Tag))
			// shard
			binary.Write(byteBuffer, binary.BigEndian, uint32(message.Shard))
			// compressor
			binary.Write(byteBuffer, binary.BigEndian, uint32(message.Compress))
			// serialize log count
			binary.Write(byteBuffer, binary.BigEndian, uint32(len(message.RawLogs)))

			// serialize logs
			for i, log := range message.RawLogs {
				binary.Write(byteBuffer, binary.BigEndian, uint32(len(log)))
				binary.Write(byteBuffer, binary.BigEndian, log)

				tunnel.outputChan[id] <- outputLog{
					ns:    ns,
					isEnd: i == len(message.ParsedLogs)-1,
					log:   byteBuffer.Bytes(),
				}
			}

		default:
			l.Logger.Panicf("%v unknown tunnel.message type: %s", tunnel, conf.Options.TunnelMessage)
		}
	}
}

func (tunnel *KafkaWriter) writeKafka() {
	// debug
	var (
		err    error
		debugF *os.File
	)

	if conf.Options.IncrSyncTunnelKafkaDebug != "" {
		fileName := fmt.Sprintf("%s-%d", conf.Options.IncrSyncTunnelKafkaDebug, tunnel.PartitionId)
		if _, err = os.Stat(fileName); os.IsNotExist(err) {
			if debugF, err = os.Create(fileName); err != nil {
				l.Logger.Panicf("%s create kafka debug file[%v] failed: %v", tunnel, fileName, err)
			}
		} else {
			if debugF, err = os.OpenFile(fileName, os.O_RDWR, 0666); err != nil {
				l.Logger.Panicf("%s open kafka debug file[%v] failed: %v", tunnel, fileName, err)
			}
		}
		defer debugF.Close()
	}

	for {
		tunnel.popIdx = (tunnel.popIdx + 1) % tunnel.encoderNr

		// read chan
		for data := range tunnel.outputChan[tunnel.popIdx] {
			if unitTestWriteKafkaFlag {
				// unit test only
				unitTestWriteKafkaChan <- data.log
			} else if conf.Options.IncrSyncTunnelKafkaDebug != "" {
				if _, err = debugF.Write(data.log); err != nil {
					l.Logger.Panicf("%s write to kafka debug file failed: %v, input data: %s", tunnel, err, data.log)
				}
				debugF.Write([]byte{10})
			} else {
				for {
					pid := int32(int(crc32.ChecksumIEEE([]byte(data.ns))) % conf.Options.TunnelKafkaPartitionNumber)
					if err = tunnel.writer.Send(data.log, pid); err != nil {
						l.Logger.Errorf("%s send [%v] with type[%v] error[%v]", tunnel, tunnel.RemoteAddr,
							conf.Options.TunnelMessage, err)

						tunnel.state = ReplyError
						time.Sleep(time.Second)
					} else {
						break
					}
				}
			}

			if data.isEnd {
				break
			}
		}
	}
}

func transformD2M(row map[string]any, key string) any {
	if _, ok := row[key]; ok {
		ds, o := row[key].(bson.D)
		if o {
			return ds.Map()
		}
	}

	return nil
}
