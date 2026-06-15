package gateway

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"thermal-recovery-flow/pkg/logger"
)

type TCPGateway struct {
	listener    net.Listener
	addr        string
	connections map[string]*TCPConnection
	mu          sync.RWMutex
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	running     atomic.Bool
	RawDataChan chan<- []byte
	logger      *logger.Logger
}

type TCPConnection struct {
	conn       net.Conn
	remoteAddr string
	lastActive time.Time
	mu         sync.Mutex
}

func NewTCPGateway(addr string, rawDataChan chan<- []byte, log *logger.Logger) *TCPGateway {
	ctx, cancel := context.WithCancel(context.Background())
	return &TCPGateway{
		addr:        addr,
		connections: make(map[string]*TCPConnection),
		ctx:         ctx,
		cancel:      cancel,
		RawDataChan: rawDataChan,
		logger:      log,
	}
}

func (g *TCPGateway) Start() error {
	listener, err := net.Listen("tcp", g.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", g.addr, err)
	}

	g.listener = listener
	g.running.Store(true)

	g.logger.Info("TCP Gateway started on %s", g.addr)

	go g.acceptConnections()

	return nil
}

func (g *TCPGateway) acceptConnections() {
	for g.running.Load() {
		select {
		case <-g.ctx.Done():
			return
		default:
		}

		conn, err := g.listener.Accept()
		if err != nil {
			if g.running.Load() {
				g.logger.Error("Failed to accept connection: %v", err)
			}
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
		go g.handleConnection(tcpConn)
	}
}

func (g *TCPGateway) handleConnection(tcpConn *TCPConnection) {
	defer func() {
		g.wg.Done()
		tcpConn.conn.Close()

		g.mu.Lock()
		delete(g.connections, tcpConn.remoteAddr)
		g.mu.Unlock()

		g.logger.Info("Connection closed from %s", tcpConn.remoteAddr)
	}()

	reader := bufio.NewReader(tcpConn.conn)
	buffer := make([]byte, 4096)

	for g.running.Load() {
		select {
		case <-g.ctx.Done():
			return
		default:
		}

		tcpConn.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := reader.Read(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			g.logger.Error("Read error from %s: %v", tcpConn.remoteAddr, err)
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
			default:
				g.logger.Warn("Raw data channel full, dropping %d bytes from %s", n, tcpConn.remoteAddr)
			}
		}
	}
}

func (g *TCPGateway) Stop() {
	if !g.running.CompareAndSwap(true, false) {
		return
	}

	g.cancel()

	if g.listener != nil {
		g.listener.Close()
	}

	g.mu.RLock()
	for _, conn := range g.connections {
		conn.conn.Close()
	}
	g.mu.RUnlock()

	g.wg.Wait()
	g.logger.Info("TCP Gateway stopped")
}

func (g *TCPGateway) GetConnectionCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.connections)
}

func (g *TCPGateway) Broadcast(data []byte) (int, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	count := 0
	for _, conn := range g.connections {
		conn.mu.Lock()
		_, err := conn.conn.Write(data)
		conn.mu.Unlock()
		if err != nil {
			g.logger.Error("Failed to broadcast to %s: %v", conn.remoteAddr, err)
			continue
		}
		count++
	}

	return count, nil
}
