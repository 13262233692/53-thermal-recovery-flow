package gateway

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"thermal-recovery-flow/internal/safety"
	"thermal-recovery-flow/pkg/logger"
)

const (
	DefaultMaxConnections  = 1024
	DefaultIdleTimeout     = 120 * time.Second
	DefaultReadBufferSize  = 4096
	AcceptBackoffThreshold = 10
	AcceptBackoffDuration  = 100 * time.Millisecond
)

type TCPGateway struct {
	listener       net.Listener
	addr           string
	maxConnections int
	idleTimeout    time.Duration
	connections    map[string]*TCPConnection
	mu             sync.RWMutex
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	running        atomic.Bool
	RawDataChan    chan<- []byte
	logger         *logger.Logger
	acceptErrors   uint64
}

type TCPConnection struct {
	conn       net.Conn
	remoteAddr string
	lastActive time.Time
	mu         sync.Mutex
	closed     atomic.Bool
}

func NewTCPGateway(addr string, rawDataChan chan<- []byte, log *logger.Logger) *TCPGateway {
	ctx, cancel := context.WithCancel(context.Background())
	return &TCPGateway{
		addr:           addr,
		maxConnections: DefaultMaxConnections,
		idleTimeout:    DefaultIdleTimeout,
		connections:    make(map[string]*TCPConnection),
		ctx:            ctx,
		cancel:         cancel,
		RawDataChan:    rawDataChan,
		logger:         log,
	}
}

func (g *TCPGateway) SetMaxConnections(n int) {
	if n > 0 {
		g.maxConnections = n
	}
}

func (g *TCPGateway) SetIdleTimeout(d time.Duration) {
	if d > 0 {
		g.idleTimeout = d
	}
}

func (g *TCPGateway) Start() error {
	listener, err := net.Listen("tcp", g.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", g.addr, err)
	}

	g.listener = listener
	g.running.Store(true)

	g.logger.Info("TCP Gateway started on %s (maxConn=%d, idleTimeout=%v)",
		g.addr, g.maxConnections, g.idleTimeout)

	safety.SafeGo(g.logger, "tcp.acceptConnections", func() {
		g.acceptConnections()
	})

	return nil
}

func (g *TCPGateway) acceptConnections() {
	defer func() {
		safety.SafeRecover(g.logger, "tcp.acceptConnections")
	}()

	for g.running.Load() {
		select {
		case <-g.ctx.Done():
			return
		default:
		}

		conn, err := g.listener.Accept()
		if err != nil {
			if !g.running.Load() {
				return
			}
			curErr := atomic.AddUint64(&g.acceptErrors, 1)
			g.logger.Error("TCP accept error #%d: %v", curErr, err)
			if curErr > AcceptBackoffThreshold {
				g.logger.Warn("Too many accept errors, backing off %v", AcceptBackoffDuration)
				select {
				case <-time.After(AcceptBackoffDuration):
				case <-g.ctx.Done():
					return
				}
			}
			continue
		}
		atomic.StoreUint64(&g.acceptErrors, 0)

		if g.GetConnectionCount() >= g.maxConnections {
			g.logger.Warn("Max connections (%d) reached, rejecting: %s",
				g.maxConnections, conn.RemoteAddr())
			conn.Close()
			continue
		}

		remoteAddr := conn.RemoteAddr().String()
		g.logger.Info("New connection from %s", remoteAddr)

		tcpConn := &TCPConnection{
			conn:       conn,
			remoteAddr: remoteAddr,
			lastActive: time.Now(),
		}

		g.mu.Lock()
		g.connections[remoteAddr] = tcpConn
		g.mu.Unlock()

		g.wg.Add(1)
		safety.SafeGoWG(g.logger, fmt.Sprintf("tcp.handleConnection[%s]", remoteAddr),
			func() { g.wg.Done() },
			func() { g.handleConnection(tcpConn) })
	}
}

func (g *TCPGateway) handleConnection(tcpConn *TCPConnection) {
	defer func() {
		safety.SafeRecover(g.logger, fmt.Sprintf("tcp.handleConnection[%s]", tcpConn.remoteAddr))
		g.cleanupConnection(tcpConn)
	}()

	reader := bufio.NewReader(tcpConn.conn)
	buffer := make([]byte, DefaultReadBufferSize)

	for g.running.Load() {
		select {
		case <-g.ctx.Done():
			return
		default:
		}

		if g.idleTimeout > 0 {
			if err := tcpConn.conn.SetReadDeadline(time.Now().Add(g.idleTimeout)); err != nil {
				g.logger.Debug("SetReadDeadline error for %s: %v", tcpConn.remoteAddr, err)
				return
			}
		} else {
			if err := tcpConn.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				return
			}
		}

		n, err := reader.Read(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				tcpConn.mu.Lock()
				idleFor := time.Since(tcpConn.lastActive)
				tcpConn.mu.Unlock()
				if g.idleTimeout > 0 && idleFor > g.idleTimeout {
					g.logger.Info("Connection %s idle for %v, closing", tcpConn.remoteAddr, idleFor)
					return
				}
				continue
			}
			if g.running.Load() {
				g.logger.Debug("Read error from %s: %v", tcpConn.remoteAddr, err)
			}
			return
		}

		if n > 0 {
			tcpConn.mu.Lock()
			tcpConn.lastActive = time.Now()
			tcpConn.mu.Unlock()

			data := make([]byte, n)
			copy(data, buffer[:n])

			select {
			case g.RawDataChan <- data:
			case <-g.ctx.Done():
				return
			default:
				g.logger.Warn("Raw data channel full, dropping %d bytes from %s", n, tcpConn.remoteAddr)
			}
		}
	}
}

func (g *TCPGateway) cleanupConnection(tcpConn *TCPConnection) {
	if tcpConn == nil {
		return
	}
	if !tcpConn.closed.CompareAndSwap(false, true) {
		return
	}

	if tcpConn.conn != nil {
		if err := tcpConn.conn.Close(); err != nil {
			g.logger.Debug("Error closing connection %s: %v", tcpConn.remoteAddr, err)
		}
	}

	g.mu.Lock()
	delete(g.connections, tcpConn.remoteAddr)
	g.mu.Unlock()

	g.logger.Info("Connection closed from %s", tcpConn.remoteAddr)
}

func (g *TCPGateway) Stop() {
	if !g.running.CompareAndSwap(true, false) {
		return
	}

	g.logger.Info("TCP Gateway stopping...")

	g.cancel()

	if g.listener != nil {
		if err := g.listener.Close(); err != nil {
			g.logger.Debug("Error closing listener: %v", err)
		}
	}

	g.mu.RLock()
	conns := make([]*TCPConnection, 0, len(g.connections))
	for _, conn := range g.connections {
		conns = append(conns, conn)
	}
	g.mu.RUnlock()

	for _, tcpConn := range conns {
		g.cleanupConnection(tcpConn)
	}

	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		g.logger.Warn("TCP Gateway shutdown timed out after 10s")
	}

	g.logger.Info("TCP Gateway stopped, total accept errors: %d",
		atomic.LoadUint64(&g.acceptErrors))
}

func (g *TCPGateway) GetConnectionCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.connections)
}

func (g *TCPGateway) Broadcast(data []byte) (int, error) {
	g.mu.RLock()
	conns := make([]*TCPConnection, 0, len(g.connections))
	for _, conn := range g.connections {
		conns = append(conns, conn)
	}
	g.mu.RUnlock()

	count := 0
	for _, tcpConn := range conns {
		if tcpConn.closed.Load() {
			continue
		}
		tcpConn.mu.Lock()
		_, err := tcpConn.conn.Write(data)
		tcpConn.mu.Unlock()
		if err != nil {
			g.logger.Debug("Failed to broadcast to %s: %v", tcpConn.remoteAddr, err)
			g.cleanupConnection(tcpConn)
			continue
		}
		tcpConn.mu.Lock()
		tcpConn.lastActive = time.Now()
		tcpConn.mu.Unlock()
		count++
	}

	return count, nil
}
