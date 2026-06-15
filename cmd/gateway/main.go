package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	"go.bug.st/serial"
	"thermal-recovery-flow/internal/choke"
	"thermal-recovery-flow/internal/config"
	"thermal-recovery-flow/internal/driftflux"
	"thermal-recovery-flow/internal/frame"
	"thermal-recovery-flow/internal/gateway"
	"thermal-recovery-flow/internal/pipeline"
	"thermal-recovery-flow/internal/protocol"
	"thermal-recovery-flow/internal/safety"
	"thermal-recovery-flow/pkg/logger"
)

type GatewayApp struct {
	cfg               *config.Config
	logger            *logger.Logger
	tcpGateway        *gateway.TCPGateway
	serialGW          *gateway.SerialGateway
	pipeline          *pipeline.Pipeline
	solver            *driftflux.DriftFluxSolver
	frameOut          *frame.FrameOutput
	chokeInterceptor  *choke.ChokeInterceptor
	statsTicker       *time.Ticker
	shutdown          atomic.Bool
}

var (
	globalPanicCount uint64
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			atomic.AddUint64(&globalPanicCount, 1)
			fmt.Fprintf(os.Stderr, "FATAL PANIC in main: %v\nStack trace:\n%s\n", r, string(stack))
			os.Exit(1)
		}
	}()

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

	log.Info("========================================================")
	log.Info("Gateway shutdown complete. Total global panics recovered: %d",
		atomic.LoadUint64(&globalPanicCount)+safety.TotalPanics())
	log.Info("========================================================")
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

	chokeChan := make(chan *choke.ChokeFrame, app.cfg.Solver.ChannelSize)
	chokeConfig := choke.DefaultChokeInterceptorConfig()
	chokeConfig.Enabled = app.cfg.Choke.Enabled
	chokeConfig.ControllerConfig.SafetyPressureLimit = app.cfg.Choke.SafetyPressureLimit
	chokeConfig.ControllerConfig.OverrideAddress = uint8(app.cfg.Choke.OverrideSlaveAddr)
	chokeConfig.ControllerConfig.OverrideRegister = uint16(app.cfg.Choke.OverrideRegister)
	chokeConfig.DownstreamPressure = app.cfg.Choke.DownstreamPressure
	chokeConfig.SenderConfig.TargetAddr = app.cfg.Choke.OverrideTargetAddr
	app.chokeInterceptor = choke.NewChokeInterceptor(chokeConfig, frameChan, chokeChan, app.logger)

	safety.SafeGo(app.logger, "app.chokeMonitor", func() {
		app.chokeMonitor(chokeChan)
	})

	if app.cfg.TCP.Enabled {
		app.tcpGateway = gateway.NewTCPGateway(app.cfg.TCP.ListenAddr, app.pipeline.Input(), app.logger)
		if app.cfg.TCP.MaxConnections > 0 {
			app.tcpGateway.SetMaxConnections(app.cfg.TCP.MaxConnections)
		}
		if app.cfg.TCP.IdleTimeoutMs > 0 {
			app.tcpGateway.SetIdleTimeout(time.Duration(app.cfg.TCP.IdleTimeoutMs) * time.Millisecond)
		}
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

	safety.SafeGo(app.logger, "app.handleErrors", func() {
		app.handleErrors(errChan)
	})

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

	if app.chokeInterceptor != nil {
		app.chokeInterceptor.Start()
	}

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
	safety.SafeGo(app.logger, "app.statsLoop", func() {
		app.statsLoop()
	})

	app.logger.Info("========================================================")
	app.logger.Info("Gateway started successfully")
	app.logger.Info("Press Ctrl+C to stop")
	app.logger.Info("========================================================")

	return nil
}

func (app *GatewayApp) stop() {
	if !app.shutdown.CompareAndSwap(false, true) {
		return
	}

	app.logger.Info("Shutting down gateway...")

	if app.statsTicker != nil {
		app.statsTicker.Stop()
	}

	if app.cfg.TCP.Enabled && app.tcpGateway != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					app.logger.Debug("Recovered during TCP gateway stop: %v", r)
				}
			}()
			app.tcpGateway.Stop()
		}()
	}

	if app.cfg.Serial.Enabled && app.serialGW != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					app.logger.Debug("Recovered during Serial gateway stop: %v", r)
				}
			}()
			app.serialGW.Stop()
		}()
	}

	if app.pipeline != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					app.logger.Debug("Recovered during pipeline stop: %v", r)
				}
			}()
			app.pipeline.Stop()
		}()
	}

	if app.chokeInterceptor != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					app.logger.Debug("Recovered during choke interceptor stop: %v", r)
				}
			}()
			app.chokeInterceptor.Stop()
		}()
	}

	if app.solver != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					app.logger.Debug("Recovered during solver stop: %v", r)
				}
			}()
			app.solver.Stop()
		}()
	}

	if app.frameOut != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					app.logger.Debug("Recovered during frame output stop: %v", r)
				}
			}()
			app.frameOut.Stop()
		}()
	}

	app.logger.Info("Gateway shutdown complete")
}

func (app *GatewayApp) handleErrors(errChan <-chan error) {
	defer func() {
		safety.SafeRecover(app.logger, "app.handleErrors")
	}()

	for err := range errChan {
		if err != nil {
			app.logger.Debug("Processing error: %v", err)
		}
	}
}

func (app *GatewayApp) chokeMonitor(chokeChan <-chan *choke.ChokeFrame) {
	defer func() {
		safety.SafeRecover(app.logger, "app.chokeMonitor")
	}()

	for cf := range chokeChan {
		if cf == nil {
			continue
		}
		if cf.ThreatLevel >= choke.ThreatCritical {
			app.logger.Warn("CHOKE: threat=%s P=%.2fMPa dP/dt=%.2e Pa/s predicted=%.2fMPa tBreach=%v opening=%.3f override=%v",
				cf.ThreatLevel,
				cf.SafetyAssessment.CurrentPressure/1e6,
				cf.PressureDerivative,
				cf.PredictedPressure/1e6,
				cf.TimeToBreach,
				cf.CurrentOpening,
				cf.OverrideSent)
		}
	}
}

func (app *GatewayApp) statsLoop() {
	defer func() {
		safety.SafeRecover(app.logger, "app.statsLoop")
	}()

	for range app.statsTicker.C {
		if app.shutdown.Load() {
			return
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					app.logger.Debug("Recovered during stats print: %v", r)
				}
			}()
			app.printStats()
		}()
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

	var serialFrames, serialBytes, serialErrors uint64
	if app.serialGW != nil {
		serialFrames, serialBytes, serialErrors = app.serialGW.GetStats()
	}

	decodedDropped, frameDropped, errDropped := app.pipeline.GetDropStats()

	app.logger.Info("------------------- Gateway Statistics -------------------")
	app.logger.Info("TCP Connections:      %d", tcpConnCount)
	app.logger.Info("TCP Output Clients:   %d", tcpClients)
	app.logger.Info("Decoder Stats:        Total: %d, Invalid: %d, Noise Filtered: %d",
		total, invalid, noise)
	app.logger.Info("Serial Stats:         Frames: %d, Bytes: %d, Errors: %d",
		serialFrames, serialBytes, serialErrors)
	app.logger.Info("Channel Backlog:      Raw: %d, Decoded: %d, Frames: %d",
		rawLen, decodedLen, frameLen)
	app.logger.Info("Drop Stats:           Decoded: %d, Frames: %d, Errors: %d",
		decodedDropped, frameDropped, errDropped)
	app.logger.Info("Solver Stats:         Total: %d, Failed: %d, Avg Iter: %.2f",
		solverStats.TotalFrames, solverStats.FailedFrames, solverStats.AvgIterations)
	app.logger.Info("Quality Range:        Min: %.4f, Max: %.4f",
		solverStats.MinQuality, solverStats.MaxQuality)
	app.logger.Info("Output Stats:         Frames: %d, Bytes: %d", framesWritten, bytesWritten)
	if app.chokeInterceptor != nil {
		chokeFrames, chokeOverrides, chokeLockdowns := app.chokeInterceptor.GetStats()
		currentOpening := app.chokeInterceptor.GetCurrentOpening()
		app.logger.Info("Choke Valve:          Frames: %d, Overrides: %d, Lockdowns: %d, Opening: %.3f",
			chokeFrames, chokeOverrides, chokeLockdowns, currentOpening)
	}
	app.logger.Info("Safety Stats:         Total recovered panics: %d",
		safety.TotalPanics())
	app.logger.Info("----------------------------------------------------------")
}

func (app *GatewayApp) waitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	for {
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP:
			func() {
				defer func() {
					if r := recover(); r != nil {
						app.logger.Debug("Recovered during SIGHUP: %v", r)
					}
				}()
				app.logger.Info("Received SIGHUP, reloading configuration...")
				app.printStats()
			}()
		case syscall.SIGINT, syscall.SIGTERM:
			app.logger.Info("Received signal %v, initiating shutdown...", sig)
			return
		}
	}
}
