package tunnel

import (
	"fmt"
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	l "github.com/alibaba/MongoShake/v2/lib/log"
	"github.com/alibaba/MongoShake/v2/oplog"
)

const (
	BatchSize = 64
	TableName = "mongoshake_mock.table"
)

var opDict = []string{"i", "d", "u", "n"}

type MockReader struct {
	generator []*FakeGenerator
}

type FakeGenerator struct {
	// not owned
	replayer []Replayer
	index    uint32
}

func (tunnel *MockReader) Link(replayer []Replayer) error {
	tunnel.generator = make([]*FakeGenerator, len(replayer))
	for i := 0; i != len(replayer); i++ {
		l.Logger.Infof("mock receiver generator-%d start", i)
		tunnel.generator[i] = &FakeGenerator{replayer: replayer}
		tunnel.generator[i].index = uint32(i)
		go tunnel.generator[i].start()
	}

	return nil
}

func (generator *FakeGenerator) start() {
	existIds := make(map[string]primitive.ObjectID, 10000000)
	for {
		var batch []*oplog.GenericOplog
		var partialLog *oplog.PartialLog

		for i := 0; i != BatchSize; i++ {
			partialLog = &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Timestamp: primitive.Timestamp{T: uint32(time.Now().Unix()), I: 0},
					Namespace: fmt.Sprintf("%s_%d", TableName, generator.index),
				},
			}
			switch nr := rand.Uint32(); {
			case nr%1000 == 0:
				// noop 0.1%
				partialLog.Operation = "n"
				partialLog.Gid = "mock-noop"
				partialLog.Object = bson.D{bson.E{Key: "mongoshake-mock", Value: "ApsaraDB"}}
			case nr%100 == 0:
				// delete 1%
				for k, oid := range existIds {
					partialLog.Operation = "d"
					partialLog.Gid = "mock-delete"
					partialLog.Object = bson.D{bson.E{Key: "_id", Value: oid}}
					delete(existIds, k)
					break
				}
			case nr%3 == 0:
				// update 30%
				for _, oid := range existIds {
					partialLog.Operation = "u"
					partialLog.Gid = "mock-update"
					// partialLog.Object = bson.M{"$set": bson.M{"updates": nr}}
					partialLog.Object = bson.D{
						bson.E{
							Key: "$set",
							Value: bson.D{
								bson.E{"updates", nr},
							},
						},
					}
					partialLog.Query = bson.D{{"_id", oid}}
					break
				}
			default:
				// insert 70%
				oid := primitive.NewObjectID()
				partialLog.Operation = "i"
				partialLog.Gid = "mock-insert"
				partialLog.Object = bson.D{
					bson.E{"_id", oid},
					bson.E{"test", "1"},
					bson.E{"abc", nr},
				}
				existIds[oid.Hex()] = oid
			}
			bytes, _ := bson.Marshal(partialLog)
			batch = append(batch, &oplog.GenericOplog{Raw: bytes})
		}
		generator.replayer[generator.index].Sync(&TMessage{
			Checksum: 0,
			Tag:      MsgRetransmission,
			Shard:    generator.index,
			Compress: 0,
			RawLogs:  oplog.LogEntryEncode(batch),
		}, nil)

		l.Logger.Infof("mock generator-index-%d generate and apply logs %d", generator.index, len(batch))
	}
}
