package kafka

import (
	"strconv"
	"time"

	"github.com/IBM/sarama"
)

type AsyncWriter struct {
	isStopped bool

	brokers  []string
	topic    string
	producer sarama.AsyncProducer

	config *Config
}

func NewAsyncWriter(rootCaFile, address string) (*AsyncWriter, error) {
	c, err := NewConfig(rootCaFile)
	if err != nil {
		return nil, err
	}

	topic, brokers, err := parse(address)
	if err != nil {
		return nil, err
	}

	s := &AsyncWriter{
		brokers: brokers,
		topic:   topic,
		config:  c,
	}

	return s, nil
}

func NewAsyncWriterWithFull(rootCaFile, address string) (*AsyncWriter, error) {
	c, err := NewConfig(rootCaFile)
	if err != nil {
		return nil, err
	}

	topic, brokers, err := parse(address)
	if err != nil {
		return nil, err
	}

	s := &AsyncWriter{
		brokers: brokers,
		topic:   topic,
		config:  c,
	}

	if s.producer, err = sarama.NewAsyncProducer(s.brokers, s.config.Config); err != nil {
		return nil, err
	}
	go s.succLoop()
	return s, nil
}

func (s *AsyncWriter) Start() error {
	producer, err := sarama.NewAsyncProducer(s.brokers, s.config.Config)
	if err != nil {
		return err
	}
	s.producer = producer
	go s.succLoop()
	return nil
}

func (s *AsyncWriter) BatchWrite(input [][]byte, pid int32) error {
	var err error
	for _, i := range input {
		err = s.send(i, pid)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AsyncWriter) Send(input []byte, pid int32) error {
	return s.send(input, pid)
}

func (s *AsyncWriter) succLoop() {
	for !s.isStopped {
		select {
		case <-s.producer.Successes():
			// do nothing
		}
	}
}

func (s *AsyncWriter) send(input []byte, pid int32) error {
	// use timestamp as key
	key := strconv.FormatInt(time.Now().UnixNano(), 16)

	msg := &sarama.ProducerMessage{
		Topic:     s.topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(input),
		Partition: pid,
	}

	s.producer.Input() <- msg
	time.Sleep(1 * time.Millisecond)
	select {
	case err := <-s.producer.Errors():
		return err
	default:
	}

	return nil
}

func (s *AsyncWriter) Close() error {
	s.isStopped = true
	return s.producer.Close()
}
