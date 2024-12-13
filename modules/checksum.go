package module

import (
	l "github.com/alibaba/MongoShake/v2/lib/log"
	"github.com/alibaba/MongoShake/v2/tunnel"
)

type ChecksumCalculator struct{}

func (coder *ChecksumCalculator) IsRegistered() bool {
	return true
}

func (coder *ChecksumCalculator) Install() bool {
	return true
}

func (coder *ChecksumCalculator) Handle(message *tunnel.WMessage) int64 {
	// write checksum value
	if len(message.RawLogs) != 0 {
		message.Checksum = message.Crc32()
		l.Logger.Debugf("Tunnel message checksum value 0x%x", message.Checksum)
	}

	return tunnel.ReplyOK
}
