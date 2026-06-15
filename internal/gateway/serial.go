package gateway

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.bug.st/serial"
	"thermal-recovery-flow/pkg/logger"
)

type SerialConfig struct {
	PortName  string
	BaudRate  int
	DataBits  int
	StopBits  serial.StopBits
	Parity    serial.Parity
	RS485Mode bool
}

type SerialGateway struct {
	port        serial.Port
	config      SerialConfig
	rawDataChan chan<- []byte
	logger      *logger.Logger
	ctx         context.Context
	cancel      context.CancelFunc
	running     atomic.Bool
	wg          sync.WaitGroup
	readBuffer  []byte
}

func NewSerialGateway(config SerialConfig, rawDataChan chan<- []byte, log *logger.Logger) *SerialGateway {
	ctx, cancel := context.WithCancel(context.Background())
	return &SerialGateway{
		config:      config,
		rawDataChan: rawDataChan,
		logger:      log,
		ctx:         ctx,
		cancel:      cancel,
		readBuffer:  make([]byte, 4096),
	}
}

func (s *SerialGateway) Start() error {
	mode := &serial.Mode{
		BaudRate: s.config.BaudRate,
		DataBits: s.config.DataBits,
		StopBits: s.config.StopBits,
		Parity:   s.config.Parity,
	}

	port, err := serial.Open(s.config.PortName, mode)
	if err != nil {
		return err
	}

	s.port = port
	s.running.Store(true)

	s.logger.Info("Serial Gateway started on %s (baud: %d, RS485: %v)",
		s.config.PortName, s.config.BaudRate, s.config.RS485Mode)

	s.wg.Add(1)
	go s.readLoop()

	return nil
}

func (s *SerialGateway) readLoop() {
	defer s.wg.Done()

	buffer := make([]byte, 4096)
	frameBuffer := make([]byte, 0, 1024)
	lastByteTime := time.Now()
	idleTimeout := 5 * time.Millisecond

	for s.running.Load() {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		s.port.SetReadTimeout(100 * time.Millisecond)
		n, err := s.port.Read(buffer)
		if err != nil {
			if s.running.Load() {
				s.logger.Error("Serial read error: %v", err)
			}
			continue
		}

		if n > 0 {
			now := time.Now()
			if now.Sub(lastByteTime) > idleTimeout && len(frameBuffer) > 0 {
				s.dispatchFrame(frameBuffer)
				frameBuffer = frameBuffer[:0]
			}

			frameBuffer = append(frameBuffer, buffer[:n]...)
			lastByteTime = now

			if len(frameBuffer) >= 256 {
				s.dispatchFrame(frameBuffer)
				frameBuffer = frameBuffer[:0]
			}
		} else if len(frameBuffer) > 0 {
			s.dispatchFrame(frameBuffer)
			frameBuffer = frameBuffer[:0]
		}
	}

	if len(frameBuffer) > 0 {
		s.dispatchFrame(frameBuffer)
	}
}

func (s *SerialGateway) dispatchFrame(data []byte) {
	frame := make([]byte, len(data))
	copy(frame, data)

	select {
	case s.rawDataChan <- frame:
	default:
		s.logger.Warn("Serial data channel full, dropping %d bytes", len(frame))
	}
}

func (s *SerialGateway) Write(data []byte) (int, error) {
	if s.port == nil {
		return 0, nil
	}
	return s.port.Write(data)
}

func (s *SerialGateway) Stop() {
	if !s.running.CompareAndSwap(true, false) {
		return
	}

	s.cancel()

	if s.port != nil {
		s.port.Close()
	}

	s.wg.Wait()
	s.logger.Info("Serial Gateway stopped on %s", s.config.PortName)
}

func DefaultSerialConfig(portName string) SerialConfig {
	return SerialConfig{
		PortName:  portName,
		BaudRate:  115200,
		DataBits:  8,
		StopBits:  serial.OneStopBit,
		Parity:    serial.NoParity,
		RS485Mode: false,
	}
}

func DefaultRS485Config(portName string) SerialConfig {
	return SerialConfig{
		PortName:  portName,
		BaudRate:  115200,
		DataBits:  8,
		StopBits:  serial.OneStopBit,
		Parity:    serial.EvenParity,
		RS485Mode: true,
	}
}
