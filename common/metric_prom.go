package utils

import "github.com/prometheus/client_golang/prometheus"

var OplogFilterProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "oplog_filter_total",
}, []string{"name", "stage"})

var OplogGetProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "oplog_get_total",
}, []string{"name", "stage"})

var OplogConsumeProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "oplog_consume_total",
}, []string{"name", "stage"})

var OplogApplyProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "oplog_apply_total",
}, []string{"name", "stage"})

var OplogSuccessProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "oplog_success_total",
}, []string{"name", "stage"})

var OplogFailProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "oplog_fail_total",
}, []string{"name", "stage"})

var OplogWriteFailProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "oplog_write_fail_total",
}, []string{"name", "stage"})

var CheckpointTimesProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "checkpoint_times",
}, []string{"name", "stage"})

var RetransmissionProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "retransmission_total",
}, []string{"name", "stage"})

var TunnelTrafficProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "tunnel_traffic_total",
}, []string{"name", "stage"})

var LSNProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "lsn",
}, []string{"name", "stage"})

var LSNAckProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "lsn_ack",
}, []string{"name", "stage"})

var LSNCheckpointProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "lsn_checkpoint",
}, []string{"name", "stage"})

var OplogMaxSizeProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "oplog_max_size",
}, []string{"name", "stage"})

var OplogAvgSizeProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "oplog_avg_size",
}, []string{"name", "stage"})

var TableOperationsProm = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "table_operations_total",
}, []string{"name", "stage", "collection"})

var ReplStatusProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "repl_status",
}, []string{"name", "stage", "status"})

var FulProgressProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_progress",
}, []string{"stage"})

var FulColNumProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_collection_num",
}, []string{"stage"})

var FulFinColNumProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_finished_collection_num",
}, []string{"stage"})

var FulProColNumProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_processing_collection_number",
}, []string{"stage"})

var FulWaitColNumProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_wait_collection_number",
}, []string{"stage"})

var FulColPerRowProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_collection_percent_row",
}, []string{"stage", "ns"})

var FulColProRowProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_collection_processed_row",
}, []string{"stage", "ns"})

var FulColTotRowProm = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "full_collection_total_row",
}, []string{"stage", "ns"})

var DiskBufferSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "on_disk_buffer_size",
}, []string{"name"})

var DiskBufferUsed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "on_disk_buffer_used",
}, []string{"name"})

var DiskEnableDiskPersist = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "on_disk_enable_disk_persist",
}, []string{"name"})

var DiskFetchStage = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "on_disk_fetch_stage",
}, []string{"name", "stage"})

var DiskWriteCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "on_disk_write_count",
}, []string{"name"})

var DiskReadCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "on_disk_read_count",
}, []string{"name"})

func InitProm() {
	prometheus.MustRegister(
		OplogFilterProm, OplogGetProm, OplogConsumeProm, OplogApplyProm, OplogSuccessProm, OplogFailProm,
		OplogWriteFailProm, CheckpointTimesProm, RetransmissionProm, TunnelTrafficProm, LSNProm,
		LSNAckProm, LSNCheckpointProm, OplogMaxSizeProm, OplogAvgSizeProm, TableOperationsProm, ReplStatusProm,
		FulProgressProm, FulColNumProm, FulFinColNumProm, FulProColNumProm, FulWaitColNumProm,
		FulColPerRowProm, FulColProRowProm, FulColTotRowProm, DiskBufferSize, DiskBufferUsed,
		DiskEnableDiskPersist, DiskFetchStage, DiskWriteCount, DiskReadCount,
	)
}
