package frame

import (
	"context"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"thermal-recovery-flow/internal/driftflux"
	"thermal-recovery-flow/internal/protocol"
	"thermal-recovery-flow/internal/safety"
	"thermal-recovery-flow/pkg/logger"
)

const (
	DefaultMaxOutputClients = 64
	DefaultClientIdleTimeout = 300 * time.Second
)

type OutputFormat int

const (
	FormatJSON OutputFormat = iota
	FormatCSV
	FormatBinary
)

type tcpClient struct {
	conn       net.Conn
	remoteAddr string
	closed     atomic.Bool
	lastActive time.Time
	mu         sync.Mutex
}

type FrameOutput struct {
	config            OutputConfig
	logger            *logger.Logger
	inputChan         <-chan *driftflux.HydrodynamicsFrame
	fileWriter        io.WriteCloser
	csvWriter         *csv.Writer
	fileClosed        atomic.Bool
	tcpListener       net.Listener
	tcpClients        map[*tcpClient]bool
	tcpMu             sync.RWMutex
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
	running           atomic.Bool
	stopped           atomic.Bool
	framesWritten     uint64
	bytesWritten      uint64
	flushTicker       *time.Ticker
	flushInterval     time.Duration
	broadcastDropped  uint64
	clientAcceptErrors uint64
}

type OutputConfig struct {
	Enabled       bool
	Format        OutputFormat
	OutputFile    string
	TCPServer     bool
	TCPListenAddr string
	FlushInterval time.Duration
	MaxClients    int
	IdleTimeout   time.Duration
}

func ParseOutputFormat(format string) OutputFormat {
	switch format {
	case "csv", "CSV":
		return FormatCSV
	case "binary", "BINARY":
		return FormatBinary
	default:
		return FormatJSON
	}
}

func NewFrameOutput(cfg OutputConfig, input <-chan *driftflux.HydrodynamicsFrame, log *logger.Logger) *FrameOutput {
	ctx, cancel := context.WithCancel(context.Background())
	if cfg.MaxClients <= 0 {
		cfg.MaxClients = DefaultMaxOutputClients
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = DefaultClientIdleTimeout
	}
	return &FrameOutput{
		config:        cfg,
		logger:        log,
		inputChan:     input,
		tcpClients:    make(map[*tcpClient]bool),
		ctx:           ctx,
		cancel:        cancel,
		flushInterval: cfg.FlushInterval,
	}
}

func (o *FrameOutput) Start() error {
	if !o.config.Enabled {
		o.logger.Info("Frame output disabled")
		return nil
	}

	if !o.running.CompareAndSwap(false, true) {
		return nil
	}
	o.stopped.Store(false)

	if o.config.OutputFile != "" {
		if err := o.openFile(); err != nil {
			o.running.Store(false)
			return err
		}
	}

	if o.config.TCPServer {
		if err := o.startTCPServer(); err != nil {
			o.logger.Error("Failed to start TCP output server: %v", err)
		}
	}

	o.wg.Add(1)
	safety.SafeGoWG(o.logger, "frame.processLoop",
		func() { o.wg.Done() },
		func() { o.processLoop() })

	if o.flushInterval > 0 {
		o.flushTicker = time.NewTicker(o.flushInterval)
		o.wg.Add(1)
		safety.SafeGoWG(o.logger, "frame.flushLoop",
			func() { o.wg.Done() },
			func() { o.flushLoop() })
	}

	o.logger.Info("Frame output started (format: %v, file: %s, tcp: %v, maxClients: %d)",
		o.config.Format, o.config.OutputFile, o.config.TCPServer, o.config.MaxClients)

	return nil
}

func (o *FrameOutput) openFile() error {
	dir := filepath.Dir(o.config.OutputFile)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create output directory: %w", err)
		}
	}

	f, err := os.OpenFile(o.config.OutputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open output file: %w", err)
	}

	o.fileWriter = f
	o.fileClosed.Store(false)

	if o.config.Format == FormatCSV {
		o.csvWriter = csv.NewWriter(f)
		header := []string{
			"timestamp", "high_speed_time", "pressure_pa", "temperature_k",
			"steam_quality", "void_fraction", "slip_velocity",
			"liquid_velocity", "vapor_velocity", "mixture_velocity",
			"liquid_density", "vapor_density", "mixture_density",
			"liquid_enthalpy", "vapor_enthalpy", "mass_flow_rate",
			"reynolds_number", "iterations", "converged",
			"slave_address", "sensor_type",
		}
		if err := o.csvWriter.Write(header); err != nil {
			o.safeCloseFile()
			return err
		}
		o.csvWriter.Flush()
		if err := o.csvWriter.Error(); err != nil {
			o.safeCloseFile()
			return err
		}
	}

	return nil
}

func (o *FrameOutput) safeCloseFile() {
	if !o.fileClosed.CompareAndSwap(false, true) {
		return
	}
	if o.csvWriter != nil {
		o.csvWriter.Flush()
		o.csvWriter = nil
	}
	if o.fileWriter != nil {
		if err := o.fileWriter.Close(); err != nil {
			o.logger.Debug("Error closing output file: %v", err)
		}
		o.fileWriter = nil
	}
}

func (o *FrameOutput) startTCPServer() error {
	listener, err := net.Listen("tcp", o.config.TCPListenAddr)
	if err != nil {
		return err
	}

	o.tcpListener = listener
	o.logger.Info("Frame output TCP server listening on %s (maxClients=%d)",
		o.config.TCPListenAddr, o.config.MaxClients)

	o.wg.Add(1)
	safety.SafeGoWG(o.logger, "frame.acceptTCPClients",
		func() { o.wg.Done() },
		func() { o.acceptTCPClients() })
	return nil
}

func (o *FrameOutput) acceptTCPClients() {
	defer func() {
		safety.SafeRecover(o.logger, "frame.acceptTCPClients")
	}()

	for o.running.Load() {
		select {
		case <-o.ctx.Done():
			return
		default:
		}

		if o.tcpListener == nil {
			return
		}

		conn, err := o.tcpListener.Accept()
		if err != nil {
			if !o.running.Load() {
				return
			}
			curErr := atomic.AddUint64(&o.clientAcceptErrors, 1)
			o.logger.Debug("Frame output TCP accept error #%d: %v", curErr, err)
			if curErr > 10 {
				select {
				case <-time.After(100 * time.Millisecond):
				case <-o.ctx.Done():
					return
				}
			}
			continue
		}
		atomic.StoreUint64(&o.clientAcceptErrors, 0)

		if o.GetClientCount() >= o.config.MaxClients {
			o.logger.Warn("Max output clients (%d) reached, rejecting: %s",
				o.config.MaxClients, conn.RemoteAddr())
			conn.Close()
			continue
		}

		remoteAddr := conn.RemoteAddr().String()
		o.logger.Info("New frame output client connected: %s", remoteAddr)

		client := &tcpClient{
			conn:       conn,
			remoteAddr: remoteAddr,
			lastActive: time.Now(),
		}

		o.tcpMu.Lock()
		o.tcpClients[client] = true
		o.tcpMu.Unlock()

		o.wg.Add(1)
		safety.SafeGoWG(o.logger, fmt.Sprintf("frame.monitorClient[%s]", remoteAddr),
			func() { o.wg.Done() },
			func() { o.monitorTCPClient(client) })
	}
}

func (o *FrameOutput) monitorTCPClient(client *tcpClient) {
	defer func() {
		safety.SafeRecover(o.logger, fmt.Sprintf("frame.monitorClient[%s]", client.remoteAddr))
		o.cleanupClient(client)
	}()

	buf := make([]byte, 1024)
	for o.running.Load() {
		select {
		case <-o.ctx.Done():
			return
		default:
		}

		if o.config.IdleTimeout > 0 {
			if err := client.conn.SetReadDeadline(time.Now().Add(o.config.IdleTimeout)); err != nil {
				return
			}
		} else {
			if err := client.conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
				return
			}
		}

		_, err := client.conn.Read(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				idleFor := time.Since(client.lastActive)
				if o.config.IdleTimeout > 0 && idleFor > o.config.IdleTimeout {
					o.logger.Info("Frame output client %s idle for %v, disconnecting",
						client.remoteAddr, idleFor)
					return
				}
				continue
			}
			if o.running.Load() {
				o.logger.Debug("Frame output client %s read error: %v", client.remoteAddr, err)
			}
			return
		}

		client.mu.Lock()
		client.lastActive = time.Now()
		client.mu.Unlock()
	}
}

func (o *FrameOutput) cleanupClient(client *tcpClient) {
	if client == nil {
		return
	}
	if !client.closed.CompareAndSwap(false, true) {
		return
	}

	o.tcpMu.Lock()
	delete(o.tcpClients, client)
	o.tcpMu.Unlock()

	if client.conn != nil {
		if err := client.conn.Close(); err != nil {
			o.logger.Debug("Error closing frame output client %s: %v", client.remoteAddr, err)
		}
	}

	o.logger.Info("Frame output client disconnected: %s", client.remoteAddr)
}

func (o *FrameOutput) processLoop() {
	defer func() {
		safety.SafeRecover(o.logger, "frame.processLoop")
	}()

	for o.running.Load() {
		select {
		case <-o.ctx.Done():
			return
		case frame, ok := <-o.inputChan:
			if !ok {
				return
			}
			if frame == nil {
				continue
			}

			data, err := o.serializeFrame(frame)
			if err != nil {
				o.logger.Error("Failed to serialize frame: %v", err)
				continue
			}

			if data != nil && len(data) > 0 {
				o.writeToFile(data)
			}

			o.broadcastToTCPClients(data)
		}
	}
}

func (o *FrameOutput) writeToFile(data []byte) {
	if o.fileClosed.Load() || o.fileWriter == nil {
		return
	}

	if _, err := o.fileWriter.Write(data); err != nil {
		o.logger.Error("Failed to write to file: %v", err)
		o.safeCloseFile()
		return
	}
	atomic.AddUint64(&o.framesWritten, 1)
	atomic.AddUint64(&o.bytesWritten, uint64(len(data)))
}

func (o *FrameOutput) serializeFrame(frame *driftflux.HydrodynamicsFrame) ([]byte, error) {
	switch o.config.Format {
	case FormatJSON:
		return o.serializeJSON(frame)
	case FormatCSV:
		return o.serializeCSV(frame)
	case FormatBinary:
		return o.serializeBinary(frame)
	default:
		return o.serializeJSON(frame)
	}
}

func (o *FrameOutput) serializeJSON(frame *driftflux.HydrodynamicsFrame) ([]byte, error) {
	type jsonFrame struct {
		Timestamp       string  `json:"timestamp"`
		HighSpeedTime   uint64  `json:"high_speed_time"`
		Pressure        float64 `json:"pressure_pa"`
		Temperature     float64 `json:"temperature_k"`
		TemperatureC    float64 `json:"temperature_c"`
		SteamQuality    float64 `json:"steam_quality"`
		VoidFraction    float64 `json:"void_fraction"`
		SlipVelocity    float64 `json:"slip_velocity_m_s"`
		LiquidVelocity  float64 `json:"liquid_velocity_m_s"`
		VaporVelocity   float64 `json:"vapor_velocity_m_s"`
		MixtureVelocity float64 `json:"mixture_velocity_m_s"`
		LiquidDensity   float64 `json:"liquid_density_kg_m3"`
		VaporDensity    float64 `json:"vapor_density_kg_m3"`
		MixtureDensity  float64 `json:"mixture_density_kg_m3"`
		LiquidEnthalpy  float64 `json:"liquid_enthalpy_j_kg"`
		VaporEnthalpy   float64 `json:"vapor_enthalpy_j_kg"`
		MassFlowRate    float64 `json:"mass_flow_rate_kg_s"`
		ReynoldsNumber  float64 `json:"reynolds_number"`
		Iterations      int     `json:"iterations"`
		Converged       bool    `json:"converged"`
		SlaveAddress    uint8   `json:"slave_address"`
		SensorType      string  `json:"sensor_type"`
	}

	sensorType := "unknown"
	switch frame.SensorType {
	case protocol.SensorVortexFlowmeter:
		sensorType = "vortex_flowmeter"
	case protocol.SensorCapacitanceProdMeter:
		sensorType = "capacitance_prod_meter"
	}

	jf := jsonFrame{
		Timestamp:       frame.Timestamp.Format(time.RFC3339Nano),
		HighSpeedTime:   frame.HighSpeedTime,
		Pressure:        frame.Pressure,
		Temperature:     frame.Temperature,
		TemperatureC:    frame.Temperature - 273.15,
		SteamQuality:    frame.SteamQuality,
		VoidFraction:    frame.VoidFraction,
		SlipVelocity:    frame.SlipVelocity,
		LiquidVelocity:  frame.LiquidVelocity,
		VaporVelocity:   frame.VaporVelocity,
		MixtureVelocity: frame.MixtureVelocity,
		LiquidDensity:   frame.LiquidDensity,
		VaporDensity:    frame.VaporDensity,
		MixtureDensity:  frame.MixtureDensity,
		LiquidEnthalpy:  frame.LiquidEnthalpy,
		VaporEnthalpy:   frame.VaporEnthalpy,
		MassFlowRate:    frame.MassFlowRate,
		ReynoldsNumber:  frame.ReynoldsNumber,
		Iterations:      frame.Iterations,
		Converged:       frame.Converged,
		SlaveAddress:    frame.SlaveAddress,
		SensorType:      sensorType,
	}

	data, err := json.Marshal(jf)
	if err != nil {
		return nil, err
	}

	return append(data, '\n'), nil
}

func (o *FrameOutput) serializeCSV(frame *driftflux.HydrodynamicsFrame) ([]byte, error) {
	if o.csvWriter == nil {
		return nil, nil
	}

	record := []string{
		frame.Timestamp.Format(time.RFC3339Nano),
		strconv.FormatUint(frame.HighSpeedTime, 10),
		strconv.FormatFloat(frame.Pressure, 'f', 6, 64),
		strconv.FormatFloat(frame.Temperature, 'f', 4, 64),
		strconv.FormatFloat(frame.SteamQuality, 'f', 8, 64),
		strconv.FormatFloat(frame.VoidFraction, 'f', 8, 64),
		strconv.FormatFloat(frame.SlipVelocity, 'f', 6, 64),
		strconv.FormatFloat(frame.LiquidVelocity, 'f', 6, 64),
		strconv.FormatFloat(frame.VaporVelocity, 'f', 6, 64),
		strconv.FormatFloat(frame.MixtureVelocity, 'f', 6, 64),
		strconv.FormatFloat(frame.LiquidDensity, 'f', 4, 64),
		strconv.FormatFloat(frame.VaporDensity, 'f', 4, 64),
		strconv.FormatFloat(frame.MixtureDensity, 'f', 4, 64),
		strconv.FormatFloat(frame.LiquidEnthalpy, 'f', 2, 64),
		strconv.FormatFloat(frame.VaporEnthalpy, 'f', 2, 64),
		strconv.FormatFloat(frame.MassFlowRate, 'f', 6, 64),
		strconv.FormatFloat(frame.ReynoldsNumber, 'f', 2, 64),
		strconv.Itoa(frame.Iterations),
		strconv.FormatBool(frame.Converged),
		strconv.FormatUint(uint64(frame.SlaveAddress), 10),
		strconv.FormatUint(uint64(frame.SensorType), 10),
	}

	if err := o.csvWriter.Write(record); err != nil {
		return nil, err
	}

	return nil, nil
}

func (o *FrameOutput) serializeBinary(frame *driftflux.HydrodynamicsFrame) ([]byte, error) {
	buf := make([]byte, 0, 128)

	buf = append(buf, byte(frame.SlaveAddress))
	buf = append(buf, byte(frame.SensorType))

	for _, v := range []float64{
		frame.Pressure, frame.Temperature,
		frame.SteamQuality, frame.VoidFraction, frame.SlipVelocity,
		frame.LiquidVelocity, frame.VaporVelocity, frame.MixtureVelocity,
		frame.LiquidDensity, frame.VaporDensity, frame.MixtureDensity,
		frame.LiquidEnthalpy, frame.VaporEnthalpy, frame.MassFlowRate,
		frame.ReynoldsNumber,
	} {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, math.Float64bits(v))
		buf = append(buf, b...)
	}

	ts := frame.Timestamp.UnixNano()
	for i := 0; i < 8; i++ {
		buf = append(buf, byte(ts>>(i*8)))
	}

	buf = append(buf, byte(frame.HighSpeedTime>>40),
		byte(frame.HighSpeedTime>>32),
		byte(frame.HighSpeedTime>>24),
		byte(frame.HighSpeedTime>>16),
		byte(frame.HighSpeedTime>>8),
		byte(frame.HighSpeedTime))

	buf = append(buf, byte(frame.Iterations))
	if frame.Converged {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}

	return buf, nil
}

func (o *FrameOutput) broadcastToTCPClients(data []byte) {
	if len(data) == 0 {
		return
	}

	o.tcpMu.RLock()
	clients := make([]*tcpClient, 0, len(o.tcpClients))
	for c := range o.tcpClients {
		clients = append(clients, c)
	}
	o.tcpMu.RUnlock()

	for _, client := range clients {
		if client.closed.Load() {
			continue
		}
		client.mu.Lock()
		_, err := client.conn.Write(data)
		if err == nil {
			client.lastActive = time.Now()
		}
		client.mu.Unlock()
		if err != nil {
			atomic.AddUint64(&o.broadcastDropped, 1)
			o.logger.Debug("Failed to broadcast to TCP client %s: %v", client.remoteAddr, err)
			o.cleanupClient(client)
		}
	}
}

func (o *FrameOutput) flushLoop() {
	defer func() {
		safety.SafeRecover(o.logger, "frame.flushLoop")
	}()

	for o.running.Load() {
		select {
		case <-o.ctx.Done():
			return
		case <-o.flushTicker.C:
			if !o.fileClosed.Load() {
				if o.csvWriter != nil {
					o.csvWriter.Flush()
					if err := o.csvWriter.Error(); err != nil {
						o.logger.Error("CSV flush error: %v", err)
					}
				}
				if o.fileWriter != nil {
					if f, ok := o.fileWriter.(*os.File); ok {
						if err := f.Sync(); err != nil {
							o.logger.Debug("File sync error: %v", err)
						}
					}
				}
			}
		}
	}
}

func (o *FrameOutput) Stop() {
	if !o.running.CompareAndSwap(true, false) {
		return
	}
	if !o.stopped.CompareAndSwap(false, true) {
		return
	}

	o.logger.Info("Frame output stopping...")

	o.cancel()

	if o.flushTicker != nil {
		o.flushTicker.Stop()
	}

	if o.tcpListener != nil {
		if err := o.tcpListener.Close(); err != nil {
			o.logger.Debug("Error closing TCP listener: %v", err)
		}
		o.tcpListener = nil
	}

	o.tcpMu.Lock()
	clients := make([]*tcpClient, 0, len(o.tcpClients))
	for c := range o.tcpClients {
		clients = append(clients, c)
	}
	o.tcpMu.Unlock()

	for _, c := range clients {
		o.cleanupClient(c)
	}

	done := make(chan struct{})
	go func() {
		o.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		o.logger.Warn("Frame output shutdown timed out after 10s")
	}

	o.safeCloseFile()

	o.logger.Info("Frame output stopped. "+
		"Frames written: %d, bytes: %d, broadcast dropped: %d, accept errors: %d",
		atomic.LoadUint64(&o.framesWritten),
		atomic.LoadUint64(&o.bytesWritten),
		atomic.LoadUint64(&o.broadcastDropped),
		atomic.LoadUint64(&o.clientAcceptErrors))
}

func (o *FrameOutput) GetClientCount() int {
	o.tcpMu.RLock()
	defer o.tcpMu.RUnlock()
	return len(o.tcpClients)
}

func (o *FrameOutput) GetStats() (frames, bytes uint64, clients int) {
	frames = atomic.LoadUint64(&o.framesWritten)
	bytes = atomic.LoadUint64(&o.bytesWritten)

	o.tcpMu.RLock()
	clients = len(o.tcpClients)
	o.tcpMu.RUnlock()

	return
}
