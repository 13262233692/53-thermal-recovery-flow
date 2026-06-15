package choke

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"thermal-recovery-flow/internal/safety"
	"thermal-recovery-flow/pkg/logger"
)

const (
	DefaultOverrideAddr      = "127.0.0.1:502"
	OverrideConnectTimeout   = 50 * time.Millisecond
	OverrideWriteTimeout     = 20 * time.Millisecond
	OverrideReadTimeout      = 50 * time.Millisecond
	OverrideRetryCount       = 3
	OverrideRetryDelay       = 5 * time.Millisecond
	MaxOverrideQueueSize     = 256
)

type OverrideSenderConfig struct {
	TargetAddr    string
	ConnectTimeout time.Duration
	WriteTimeout   time.Duration
	RetryCount     int
	RetryDelay     time.Duration
}

func DefaultOverrideSenderConfig() OverrideSenderConfig {
	return OverrideSenderConfig{
		TargetAddr:     DefaultOverrideAddr,
		ConnectTimeout: OverrideConnectTimeout,
		WriteTimeout:   OverrideWriteTimeout,
		RetryCount:     OverrideRetryCount,
		RetryDelay:     OverrideRetryDelay,
	}
}

type SendResult struct {
	Packet     OverridePacket
	Success    bool
	SendTime   time.Duration
	Error      error
	Retries    int
	RemoteAddr string
}

type OverrideSender struct {
	config      OverrideSenderConfig
	logger      *logger.Logger
	ctx         context.Context
	cancel      context.CancelFunc
	running     atomic.Bool
	wg          sync.WaitGroup
	overrideChan chan OverridePacket
	resultChan   chan SendResult
	sentCount   uint64
	failCount   uint64
	totalLatency uint64
}

func NewOverrideSender(config OverrideSenderConfig, log *logger.Logger) *OverrideSender {
	ctx, cancel := context.WithCancel(context.Background())
	return &OverrideSender{
		config:       config,
		logger:       log,
		ctx:          ctx,
		cancel:       cancel,
		overrideChan: make(chan OverridePacket, MaxOverrideQueueSize),
		resultChan:   make(chan SendResult, MaxOverrideQueueSize),
	}
}

func (s *OverrideSender) Start() {
	if !s.running.CompareAndSwap(false, true) {
		return
	}
	s.wg.Add(1)
	safety.SafeGoWG(s.logger, "choke.overrideSender",
		func() { s.wg.Done() },
		func() { s.sendLoop() })
	s.logger.Info("Choke Override Sender started (target: %s)", s.config.TargetAddr)
}

func (s *OverrideSender) Stop() {
	if !s.running.CompareAndSwap(true, false) {
		return
	}
	s.cancel()
	close(s.overrideChan)

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.logger.Warn("Override sender shutdown timed out")
	}

	s.logger.Info("Choke Override Sender stopped (sent: %d, failed: %d)",
		atomic.LoadUint64(&s.sentCount), atomic.LoadUint64(&s.failCount))
}

func (s *OverrideSender) Send(packet OverridePacket) bool {
	if !s.running.Load() {
		return false
	}

	select {
	case s.overrideChan <- packet:
		return true
	default:
		s.logger.Warn("Override queue full, dropping emergency packet #%d",
			packet.Command.SequenceNumber)
		return false
	}
}

func (s *OverrideSender) EmergencySend(packet OverridePacket) SendResult {
	return s.sendPacket(packet)
}

func (s *OverrideSender) Results() <-chan SendResult {
	return s.resultChan
}

func (s *OverrideSender) sendLoop() {
	defer func() {
		safety.SafeRecover(s.logger, "choke.overrideSender")
	}()

	for s.running.Load() {
		select {
		case <-s.ctx.Done():
			return
		case packet, ok := <-s.overrideChan:
			if !ok {
				return
			}
			result := s.sendPacket(packet)
			s.emitResult(result)
		}
	}
}

func (s *OverrideSender) sendPacket(packet OverridePacket) SendResult {
	start := time.Now()
	result := SendResult{
		Packet: packet,
	}

	var lastErr error
	for attempt := 0; attempt <= s.config.RetryCount; attempt++ {
		result.Retries = attempt

		conn, err := net.DialTimeout("tcp", s.config.TargetAddr, s.config.ConnectTimeout)
		if err != nil {
			lastErr = err
			if attempt < s.config.RetryCount {
				time.Sleep(s.config.RetryDelay)
				continue
			}
			break
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					s.logger.Debug("Recovered during override conn close: %v", r)
				}
				conn.Close()
			}()

			if err := conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout)); err != nil {
				lastErr = err
				return
			}

			_, err = conn.Write(packet.RawBytes)
			if err != nil {
				lastErr = err
				return
			}

			if err := conn.SetReadDeadline(time.Now().Add(OverrideReadTimeout)); err != nil {
				lastErr = err
				return
			}

			response := make([]byte, 256)
			_, err = conn.Read(response)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// 读取超时在写命令场景下可以接受，命令可能已经成功
				} else {
					lastErr = err
					return
				}
			}

			result.Success = true
			result.RemoteAddr = conn.RemoteAddr().String()
		}()

		if result.Success {
			break
		}

		if attempt < s.config.RetryCount {
			time.Sleep(s.config.RetryDelay)
		}
	}

	result.SendTime = time.Since(start)
	result.Error = lastErr

	if result.Success {
		atomic.AddUint64(&s.sentCount, 1)
	} else {
		atomic.AddUint64(&s.failCount, 1)
	}

	return result
}

func (s *OverrideSender) emitResult(result SendResult) {
	select {
	case s.resultChan <- result:
	default:
	}
}

func (s *OverrideSender) GetStats() (sent, failed uint64) {
	return atomic.LoadUint64(&s.sentCount), atomic.LoadUint64(&s.failCount)
}
