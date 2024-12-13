package conf

type Configuration struct {
	Tunnel            string `config:"tunnel"`
	TunnelAddress     string `config:"tunnel.address"`
	SystemProfilePort int    `config:"system_profile_port"`
	ReplayerNum       int    `config:"replayer"`

	LogDirectory string `config:"log.dir"`
	LogLevel     string `config:"log.level"`
	LogFileName  string `config:"log.file"`
	LogFlush     bool   `config:"log.flush"`
	LogMaxSizeMb int    `config:"log.max_size_mb"`
	LogMaxBackup int    `config:"log.max_backup"`
	LogMaxAge    int    `config:"log.max_age"`
}

var Options Configuration
