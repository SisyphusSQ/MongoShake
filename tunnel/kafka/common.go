package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	prometheusmetrics "github.com/deathowl/go-metrics-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rcrowley/go-metrics"

	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/lib/log"
)

var (
	nsDefault                = "mongoshake"
	topicDefault             = "mongoshake"
	topicSplitter            = "@"
	brokersSplitter          = ","
	defaultPartition   int32 = 0
	metricsRegistry          = metrics.NewRegistry()
	prometheusRegistry       = prometheus.DefaultRegisterer
)

type Message struct {
	Key       []byte
	Value     []byte
	Offset    int64
	TimeStamp time.Time
}

type Config struct {
	Config *sarama.Config
}

func NewConfig(rootCaFile string) (*Config, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_2_0_0
	config.MetricRegistry = metricsRegistry

	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = 16*utils.MB + 2*utils.MB // 2MB for the reserve gap

	// for async
	config.Producer.Flush.Frequency = 1 * time.Second
	config.Producer.Flush.MaxMessages = 1000
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Timeout = 10 * time.Second

	// ssl
	if rootCaFile != "" {
		sslConfig := &tls.Config{
			InsecureSkipVerify: true,
		}
		caCert, err := os.ReadFile(rootCaFile)
		if err != nil {
			l.Logger.Errorf("failed to load the ca cert file: %s failed: %s", rootCaFile, err.Error())
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		sslConfig.RootCAs = caCertPool
		config.Net.TLS.Config = sslConfig
		config.Net.TLS.Enable = true
	}

	pClient := prometheusmetrics.NewPrometheusProvider(metricsRegistry, nsDefault, "", prometheusRegistry, 1*time.Second)
	go pClient.UpdatePrometheusMetrics()

	return &Config{
		Config: config,
	}, nil
}

// parse the address (topic@broker1,broker2,...)
func parse(address string) (string, []string, error) {
	arr := strings.Split(address, topicSplitter)
	le := len(arr)
	if le == 0 || le > 2 {
		return "", nil, fmt.Errorf("address format error")
	}

	topic := topicDefault
	if le == 2 {
		topic = arr[0]
	}

	brokers := strings.Split(arr[le-1], brokersSplitter)
	return topic, brokers, nil
}
