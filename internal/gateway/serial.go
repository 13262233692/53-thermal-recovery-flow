package gateway

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.bug.st/serial"
	"thermal-recovery-flow/internal/safety"
	"thermal-recovery-flow/pkg/logger"
)

const (
	DefaultReadTimeout       = 100 * time.Millisecond
	DefaultSerialIdleTimeout = 5 * time.Millisecond
	MaxFrameBufferSize       = 4096
	DefaultSerialBuffer      = 4096
	SerialErrorThreshold     = 100
	SerialErrorBackoff       = 50 * time.Millisecond
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
	port          serial.Port
	config        SerialConfig
	rawDataChan   chan<- []byte
	logger        *logger.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	running       atomic.Bool
	wg            sync.WaitGroup
	readBuffer    []byte
	closed        atomic.Bool
	totalErrors   uint64
	framesSent    uint64
	bytesSent     uint64
}

func NewSerialGateway(config SerialConfig, rawDataChan chan<- []byte, log *logger.Logger) *SerialGateway {
	ctx, cancel := context.WithCancel(context.Background())
	return &SerialGateway{
		config:      config,
		rawDataChan: rawDataChan,
		logger:      log,
		ctx:         ctx,
		cancel:      cancel,
		readBuffer:  make([]byte, DefaultSerialBuffer),
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
	s.closed.Store(false)

	s.logger.Info("Serial Gateway started on %s (baud: %d, RS485: %v)",
		s.config.PortName, s.config.BaudRate, s.config.RS485Mode)

	s.wg.Add(1)
	safety.SafeGoWG(s.logger, "serial.readLoop",
		func() { s.wg.Done() },
		func() { s.readLoop() })

	return nil
}

func (s *SerialGateway) readLoop() {
	defer func() {
		safety.SafeRecover(s.logger, "serial.readLoop")
	}()

	buffer := make([]byte, DefaultSerialBuffer)
	frameBuffer := make([]byte, 0, 1024)
	lastByteTime := time.Now()
	idleTimeout := DefaultSerialIdleTimeout

	for s.running.Load() {
		select {
		case <-s.ctx.Done():
			if len(frameBuffer) > 0 {
				s.dispatchFrame(frameBuffer)
				frameBuffer = nil
			}
			return
		default:
		}

		if s.port == nil {
			return
		}

		if err := s.port.SetReadTimeout(DefaultReadTimeout); err != nil {
			atomic.AddUint64(&s.totalErrors, 1)
			s.logger.Debug("Serial SetReadTimeout error: %v", err)
			time.Sleep(SerialErrorBackoff)
			continue
		}

		n, err := s.port.Read(buffer)
		if err != nil {
			if !s.running.Load() {
				if len(frameBuffer) > 0 {
					s.dispatchFrame(frameBuffer)
					frameBuffer = nil
				}
				return
			}
			curErr := atomic.AddUint64(&s.totalErrors, 1)
			s.logger.Debug("Serial read error #%d: %v", curErr, err)
			if curErr > SerialErrorThreshold {
				s.logger.Warn("Serial too many errors (%d), backing off %v",
					curErr, SerialErrorBackoff)
				select {
				case <-time.After(SerialErrorBackoff):
				case <-s.ctx.Done():
					if len(frameBuffer) > 0 {
						s.dispatchFrame(frameBuffer)
						frameBuffer = nil
					}
					return
				}
			}
			continue
		}
		atomic.StoreUint64(&s.totalErrors, 0)

		if n > 0 {
			now := time.Now()
			if now.Sub(lastByteTime) > idleTimeout && len(frameBuffer) > 0 {
				s.dispatchFrame(frameBuffer)
				frameBuffer = frameBuffer[:0]
			}

			if len(frameBuffer)+n > MaxFrameBufferSize {
				s.logger.Warn("Serial frame buffer overflow (%d bytes), truncating",
					len(frameBuffer)+n)
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
		frameBuffer = nil
	}
}

func (s *SerialGateway) dispatchFrame(data []byte) {
	if len(data) == 0 {
		return
	}

	frame := make([]byte, len(data))
	copy(frame, data)

	select {
	case s.rawDataChan <- frame:
		atomic.AddUint64(&s.framesSent, 1)
		atomic.AddUint64(&s.bytesSent, uint64(len(frame)))
	case <-s.ctx.Done():
		return
	default:
		s.logger.Warn("Serial data channel full, dropping %d bytes", len(frame))
	}
}

func (s *SerialGateway) Write(data []byte) (int, error) {
	if s.port == nil || s.closed.Load() {
		return 0, nil
	}
	return s.port.Write(data)
}

func (s *SerialGateway) GetStats() (frames, bytes, errors uint64) {
	frames = atomic.LoadUint64(&s.framesSent)
	bytes = atomic.LoadUint64(&s.bytesSent)
	errors = atomic.LoadUint64(&s.totalErrors)
	return
}

func (s *SerialGateway) Stop() {
	if !s.running.CompareAndSwap(true, false) {
		return
	}

	s.logger.Info("Serial Gateway stopping on %s...", s.config.PortName)

	s.cancel()

	s.muSafeClosePort()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.logger.Warn("Serial Gateway shutdown timed out after 5s")
	}

	s.logger.Info("Serial Gateway stopped on %s (frames: %d, bytes: %d, errors: %d)",
		s.config.PortName,
		atomic.LoadUint64(&s.framesSent),
		atomic.LoadUint64(&s.bytesSent),
		atomic.LoadUint64(&s.totalErrors))
}

func (s *SerialGateway) muSafeClosePort() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	if s.port != nil {
		if err := s.port.Close(); err != nil {
			s.logger.Debug("Error closing serial port: %v", err)
		}
		s.port = nil
	}
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
