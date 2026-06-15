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
	"thermal-recovery-flow/pkg/logger"
)

type OutputFormat int

const (
	FormatJSON OutputFormat = iota
	FormatCSV
	FormatBinary
)

type FrameOutput struct {
	config         OutputConfig
	logger         *logger.Logger
	inputChan      <-chan *driftflux.HydrodynamicsFrame
	fileWriter     io.WriteCloser
	csvWriter      *csv.Writer
	tcpListener    net.Listener
	tcpClients     map[net.Conn]bool
	tcpMu          sync.RWMutex
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	running        atomic.Bool
	framesWritten  uint64
	bytesWritten   uint64
	flushTicker    *time.Ticker
	flushInterval  time.Duration
}

type OutputConfig struct {
	Enabled       bool
	Format        OutputFormat
	OutputFile    string
	TCPServer     bool
	TCPListenAddr string
	FlushInterval time.Duration
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
	return &FrameOutput{
		config:        cfg,
		logger:        log,
		inputChan:     input,
		tcpClients:    make(map[net.Conn]bool),
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

	if o.config.OutputFile != "" {
		if err := o.openFile(); err != nil {
			return err
		}
	}

	if o.config.TCPServer {
		if err := o.startTCPServer(); err != nil {
			o.logger.Error("Failed to start TCP output server: %v", err)
		}
	}

	o.wg.Add(1)
	go o.processLoop()

	if o.flushInterval > 0 {
		o.flushTicker = time.NewTicker(o.flushInterval)
		o.wg.Add(1)
		go o.flushLoop()
	}

	o.logger.Info("Frame output started (format: %v, file: %s, tcp: %v)",
		o.config.Format, o.config.OutputFile, o.config.TCPServer)

	return nil
}

func (o *FrameOutput) openFile() error {
	dir := filepath.Dir(o.config.OutputFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	f, err := os.OpenFile(o.config.OutputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open output file: %w", err)
	}

	o.fileWriter = f

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
			return err
		}
		o.csvWriter.Flush()
	}

	return nil
}

func (o *FrameOutput) startTCPServer() error {
	listener, err := net.Listen("tcp", o.config.TCPListenAddr)
	if err != nil {
		return err
	}

	o.tcpListener = listener
	o.logger.Info("Frame output TCP server listening on %s", o.config.TCPListenAddr)

	go o.acceptTCPClients()
	return nil
}

func (o *FrameOutput) acceptTCPClients() {
	for o.running.Load() {
		select {
		case <-o.ctx.Done():
			return
		default:
		}

		conn, err := o.tcpListener.Accept()
		if err != nil {
			if o.running.Load() {
				o.logger.Error("TCP output accept error: %v", err)
			}
			continue
		}

		o.logger.Info("New frame output client connected: %s", conn.RemoteAddr())

		o.tcpMu.Lock()
		o.tcpClients[conn] = true
		o.tcpMu.Unlock()

		go o.monitorTCPClient(conn)
	}
}

func (o *FrameOutput) monitorTCPClient(conn net.Conn) {
	buf := make([]byte, 1024)
	for o.running.Load() {
		_, err := conn.Read(buf)
		if err != nil {
			o.tcpMu.Lock()
			delete(o.tcpClients, conn)
			o.tcpMu.Unlock()
			conn.Close()
			o.logger.Info("Frame output client disconnected: %s", conn.RemoteAddr())
			return
		}
	}
}

func (o *FrameOutput) processLoop() {
	defer o.wg.Done()

	for o.running.Load() {
		select {
		case <-o.ctx.Done():
			return
		case frame, ok := <-o.inputChan:
			if !ok {
				return
			}

			data, err := o.serializeFrame(frame)
			if err != nil {
				o.logger.Error("Failed to serialize frame: %v", err)
				continue
			}

			if o.fileWriter != nil {
				if _, err := o.fileWriter.Write(data); err != nil {
					o.logger.Error("Failed to write to file: %v", err)
				} else {
					atomic.AddUint64(&o.framesWritten, 1)
					atomic.AddUint64(&o.bytesWritten, uint64(len(data)))
				}
			}

			o.broadcastToTCPClients(data)
		}
	}
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

	if o.csvWriter != nil {
		if err := o.csvWriter.Write(record); err != nil {
			return nil, err
		}
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
	o.tcpMu.RLock()
	defer o.tcpMu.RUnlock()

	for conn := range o.tcpClients {
		if _, err := conn.Write(data); err != nil {
			o.logger.Debug("Failed to write to TCP client %s: %v", conn.RemoteAddr(), err)
		}
	}
}

func (o *FrameOutput) flushLoop() {
	defer o.wg.Done()

	for o.running.Load() {
		select {
		case <-o.ctx.Done():
			return
		case <-o.flushTicker.C:
			if o.csvWriter != nil {
				o.csvWriter.Flush()
			}
			if o.fileWriter != nil {
				if f, ok := o.fileWriter.(*os.File); ok {
					f.Sync()
				}
			}
		}
	}
}

func (o *FrameOutput) Stop() {
	if !o.running.CompareAndSwap(true, false) {
		return
	}

	o.cancel()

	if o.flushTicker != nil {
		o.flushTicker.Stop()
	}

	if o.tcpListener != nil {
		o.tcpListener.Close()
	}

	o.tcpMu.Lock()
	for conn := range o.tcpClients {
		conn.Close()
	}
	o.tcpClients = nil
	o.tcpMu.Unlock()

	o.wg.Wait()

	if o.csvWriter != nil {
		o.csvWriter.Flush()
	}

	if o.fileWriter != nil {
		o.fileWriter.Close()
	}

	o.logger.Info("Frame output stopped. Frames written: %d, bytes: %d",
		atomic.LoadUint64(&o.framesWritten), atomic.LoadUint64(&o.bytesWritten))
}

func (o *FrameOutput) GetStats() (frames, bytes uint64, clients int) {
	frames = atomic.LoadUint64(&o.framesWritten)
	bytes = atomic.LoadUint64(&o.bytesWritten)

	o.tcpMu.RLock()
	clients = len(o.tcpClients)
	o.tcpMu.RUnlock()

	return
}
