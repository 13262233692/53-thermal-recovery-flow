package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.bug.st/serial"
	"thermal-recovery-flow/internal/config"
	"thermal-recovery-flow/internal/driftflux"
	"thermal-recovery-flow/internal/frame"
	"thermal-recovery-flow/internal/gateway"
	"thermal-recovery-flow/internal/pipeline"
	"thermal-recovery-flow/internal/protocol"
	"thermal-recovery-flow/pkg/logger"
)

type GatewayApp struct {
	cfg        *config.Config
	logger     *logger.Logger
	tcpGateway *gateway.TCPGateway
	serialGW   *gateway.SerialGateway
	pipeline   *pipeline.Pipeline
	solver     *driftflux.DriftFluxSolver
	frameOut   *frame.FrameOutput
	statsTicker *time.Ticker
}

func main() {
	configPath := flag.String("config", "config.json", "Path to configuration file")
	flag.Parse()

	log := logger.Default()
	defer log.Close()

	log.Info("Thermal Recovery Flow - Industrial Data Demodulation Gateway")
	log.Info("========================================================")

	cfg, err := config.LoadConfig(*configPath, log)
	if err != nil {
		log.Fatal("Failed to load configuration: %v", err)
	}

	logLevel := config.ParseLogLevel(cfg.LogLevel)
	log.SetLevel(logLevel)

	if *configPath != "" {
		if _, err := os.Stat(*configPath); os.IsNotExist(err) {
			cfg.Save(*configPath)
			log.Info("Default configuration saved to %s", *configPath)
		}
	}

	app := &GatewayApp{
		cfg:    cfg,
		logger: log,
	}

	if err := app.init(); err != nil {
		log.Fatal("Failed to initialize application: %v", err)
	}
	defer app.stop()

	if err := app.start(); err != nil {
		log.Fatal("Failed to start application: %v", err)
	}

	app.waitForShutdown()
}

func (app *GatewayApp) init() error {
	app.logger.Info("Initializing gateway components...")

	solverConfig := driftflux.DriftFluxConfig{
		MaxIterations: app.cfg.Solver.MaxIterations,
		Tolerance:     app.cfg.Solver.Tolerance,
		FlowArea:      app.cfg.Solver.FlowArea,
		Gravity:       app.cfg.Solver.Gravity,
		C0:            app.cfg.Solver.C0,
		Vdj:           app.cfg.Solver.Vdj,
	}

	decodedChan := make(chan *protocol.RawSensorData, app.cfg.Solver.ChannelSize)
	frameChan := make(chan *driftflux.HydrodynamicsFrame, app.cfg.Solver.ChannelSize)
	errChan := make(chan error, 10000)

	app.solver = driftflux.NewDriftFluxSolver(solverConfig, decodedChan, frameChan, errChan, app.logger)

	pipelineCfg := pipeline.DefaultPipelineConfig()
	pipelineCfg.DecoderWorkers = 4
	pipelineCfg.SolverWorkers = 4
	app.pipeline = pipeline.NewPipeline(pipelineCfg, app.solver, app.logger, decodedChan, frameChan, errChan)

	outputCfg := frame.OutputConfig{
		Enabled:       app.cfg.Output.Enabled,
		Format:        frame.ParseOutputFormat(app.cfg.Output.Format),
		OutputFile:    app.cfg.Output.OutputFile,
		TCPServer:     app.cfg.Output.TCPServer,
		TCPListenAddr: app.cfg.Output.TCPListenAddr,
		FlushInterval: time.Duration(app.cfg.Output.FlushInterval) * time.Millisecond,
	}
	app.frameOut = frame.NewFrameOutput(outputCfg, frameChan, app.logger)

	if app.cfg.TCP.Enabled {
		app.tcpGateway = gateway.NewTCPGateway(app.cfg.TCP.ListenAddr, app.pipeline.Input(), app.logger)
	}

	if app.cfg.Serial.Enabled {
		serialCfg := gateway.SerialConfig{
			PortName:  app.cfg.Serial.PortName,
			BaudRate:  app.cfg.Serial.BaudRate,
			DataBits:  app.cfg.Serial.DataBits,
			StopBits:  serial.OneStopBit,
			Parity:    serial.NoParity,
			RS485Mode: app.cfg.Serial.RS485Mode,
		}

		switch app.cfg.Serial.StopBits {
		case 2:
			serialCfg.StopBits = serial.TwoStopBits
		default:
			serialCfg.StopBits = serial.OneStopBit
		}

		switch app.cfg.Serial.Parity {
		case "E", "e":
			serialCfg.Parity = serial.EvenParity
		case "O", "o":
			serialCfg.Parity = serial.OddParity
		default:
			serialCfg.Parity = serial.NoParity
		}

		app.serialGW = gateway.NewSerialGateway(serialCfg, app.pipeline.Input(), app.logger)
	}

	go app.handleErrors(errChan)

	app.logger.Info("All components initialized successfully")
	return nil
}

func (app *GatewayApp) start() error {
	app.logger.Info("Starting gateway components...")

	if err := app.frameOut.Start(); err != nil {
		return fmt.Errorf("failed to start frame output: %w", err)
	}

	app.pipeline.Start()
	app.solver.Start()

	if app.cfg.TCP.Enabled && app.tcpGateway != nil {
		if err := app.tcpGateway.Start(); err != nil {
			return fmt.Errorf("failed to start TCP gateway: %w", err)
		}
		app.logger.Info("TCP Gateway listening on %s", app.cfg.TCP.ListenAddr)
	}

	if app.cfg.Serial.Enabled && app.serialGW != nil {
		if err := app.serialGW.Start(); err != nil {
			app.logger.Warn("Failed to start Serial gateway: %v", err)
		} else {
			app.logger.Info("Serial Gateway started on %s", app.cfg.Serial.PortName)
		}
	}

	app.statsTicker = time.NewTicker(10 * time.Second)
	go app.statsLoop()

	app.logger.Info("========================================================")
	app.logger.Info("Gateway started successfully")
	app.logger.Info("Press Ctrl+C to stop")
	app.logger.Info("========================================================")

	return nil
}

func (app *GatewayApp) stop() {
	app.logger.Info("Shutting down gateway...")

	if app.statsTicker != nil {
		app.statsTicker.Stop()
	}

	if app.cfg.TCP.Enabled && app.tcpGateway != nil {
		app.tcpGateway.Stop()
	}

	if app.cfg.Serial.Enabled && app.serialGW != nil {
		app.serialGW.Stop()
	}

	if app.pipeline != nil {
		app.pipeline.Stop()
	}

	if app.solver != nil {
		app.solver.Stop()
	}

	if app.frameOut != nil {
		app.frameOut.Stop()
	}

	app.logger.Info("Gateway shutdown complete")
}

func (app *GatewayApp) handleErrors(errChan <-chan error) {
	for err := range errChan {
		if err != nil {
			app.logger.Debug("Processing error: %v", err)
		}
	}
}

func (app *GatewayApp) statsLoop() {
	for range app.statsTicker.C {
		app.printStats()
	}
}

func (app *GatewayApp) printStats() {
	var tcpConnCount int
	if app.tcpGateway != nil {
		tcpConnCount = app.tcpGateway.GetConnectionCount()
	}

	total, invalid, noise := app.pipeline.GetDecoderStats()
	rawLen, decodedLen, frameLen := app.pipeline.GetStats()
	solverStats := app.solver.GetStats()
	framesWritten, bytesWritten, tcpClients := app.frameOut.GetStats()

	app.logger.Info("------------------- Gateway Statistics -------------------")
	app.logger.Info("TCP Connections:      %d", tcpConnCount)
	app.logger.Info("TCP Output Clients:   %d", tcpClients)
	app.logger.Info("Decoder Stats:        Total: %d, Invalid: %d, Noise Filtered: %d", total, invalid, noise)
	app.logger.Info("Channel Backlog:      Raw: %d, Decoded: %d, Frames: %d", rawLen, decodedLen, frameLen)
	app.logger.Info("Solver Stats:         Total: %d, Failed: %d, Avg Iter: %.2f",
		solverStats.TotalFrames, solverStats.FailedFrames, solverStats.AvgIterations)
	app.logger.Info("Quality Range:        Min: %.4f, Max: %.4f", solverStats.MinQuality, solverStats.MaxQuality)
	app.logger.Info("Output Stats:         Frames: %d, Bytes: %d", framesWritten, bytesWritten)
	app.logger.Info("----------------------------------------------------------")
}

func (app *GatewayApp) waitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	for {
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP:
			app.logger.Info("Received SIGHUP, reloading configuration...")
			app.printStats()
		case syscall.SIGINT, syscall.SIGTERM:
			app.logger.Info("Received signal %v, initiating shutdown...", sig)
			return
		}
	}
}
