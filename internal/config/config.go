package config

import (
	"encoding/json"
	"os"

	"thermal-recovery-flow/pkg/logger"
)

type TCPConfig struct {
	Enabled    bool   `json:"enabled"`
	ListenAddr string `json:"listen_addr"`
	BufferSize int    `json:"buffer_size"`
}

type SerialPortConfig struct {
	Enabled   bool   `json:"enabled"`
	PortName  string `json:"port_name"`
	BaudRate  int    `json:"baud_rate"`
	DataBits  int    `json:"data_bits"`
	StopBits  int    `json:"stop_bits"`
	Parity    string `json:"parity"`
	RS485Mode bool   `json:"rs485_mode"`
}

type DecoderConfig struct {
	ChannelSize int `json:"channel_size"`
}

type SolverConfig struct {
	MaxIterations int     `json:"max_iterations"`
	Tolerance     float64 `json:"tolerance"`
	FlowArea      float64 `json:"flow_area"`
	Gravity       float64 `json:"gravity"`
	C0            float64 `json:"c0"`
	Vdj           float64 `json:"vdj"`
	ChannelSize   int     `json:"channel_size"`
}

type OutputConfig struct {
	Enabled        bool   `json:"enabled"`
	Format         string `json:"format"`
	OutputFile     string `json:"output_file"`
	TCPServer      bool   `json:"tcp_server"`
	TCPListenAddr  string `json:"tcp_listen_addr"`
	FlushInterval  int    `json:"flush_interval_ms"`
}

type Config struct {
	TCP      TCPConfig       `json:"tcp"`
	Serial   SerialPortConfig `json:"serial"`
	Decoder  DecoderConfig   `json:"decoder"`
	Solver   SolverConfig    `json:"solver"`
	Output   OutputConfig    `json:"output"`
	LogLevel string          `json:"log_level"`
	LogFile  string          `json:"log_file"`
}

func DefaultConfig() *Config {
	return &Config{
		TCP: TCPConfig{
			Enabled:    true,
			ListenAddr: ":502",
			BufferSize: 10000,
		},
		Serial: SerialPortConfig{
			Enabled:   false,
			PortName:  "COM1",
			BaudRate:  115200,
			DataBits:  8,
			StopBits:  1,
			Parity:    "N",
			RS485Mode: false,
		},
		Decoder: DecoderConfig{
			ChannelSize: 10000,
		},
		Solver: SolverConfig{
			MaxIterations: 100,
			Tolerance:     1e-10,
			FlowArea:      0.007854,
			Gravity:       9.81,
			C0:            1.2,
			Vdj:           0.35,
			ChannelSize:   10000,
		},
		Output: OutputConfig{
			Enabled:       true,
			Format:        "json",
			OutputFile:    "data/hydrodynamics.log",
			TCPServer:     true,
			TCPListenAddr: ":8080",
			FlushInterval: 100,
		},
		LogLevel: "INFO",
		LogFile:  "logs/gateway.log",
	}
}

func LoadConfig(path string, log *logger.Logger) (*Config, error) {
	if path == "" {
		log.Info("No config file specified, using default configuration")
		return DefaultConfig(), nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Warn("Config file not found, using default configuration")
			return DefaultConfig(), nil
		}
		return nil, err
	}

	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		log.Error("Failed to parse config file: %v", err)
		return nil, err
	}

	log.Info("Configuration loaded from %s", path)
	return cfg, nil
}

func (c *Config) Save(path string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func ParseLogLevel(level string) logger.LogLevel {
	switch level {
	case "DEBUG", "debug":
		return logger.LevelDebug
	case "INFO", "info":
		return logger.LevelInfo
	case "WARN", "warn", "WARNING", "warning":
		return logger.LevelWarn
	case "ERROR", "error":
		return logger.LevelError
	case "FATAL", "fatal":
		return logger.LevelFatal
	default:
		return logger.LevelInfo
	}
}
