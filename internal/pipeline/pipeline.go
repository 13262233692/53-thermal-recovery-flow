package pipeline

import (
	"context"
	"sync"
	"sync/atomic"

	"thermal-recovery-flow/internal/driftflux"
	"thermal-recovery-flow/internal/protocol"
	"thermal-recovery-flow/pkg/logger"
)

type Pipeline struct {
	rawDataChan    chan []byte
	decodedChan    chan *protocol.RawSensorData
	hydroFrameChan chan *driftflux.HydrodynamicsFrame
	errorChan      chan error
	decoder        *protocol.BitDecoder
	solver         *driftflux.DriftFluxSolver
	logger         *logger.Logger
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	running        atomic.Bool
	decoderCount   int
	solverCount    int
}

type PipelineConfig struct {
	RawBufferSize     int
	DecodedBufferSize int
	FrameBufferSize   int
	ErrorBufferSize   int
	DecoderWorkers    int
	SolverWorkers     int
}

func DefaultPipelineConfig() PipelineConfig {
	return PipelineConfig{
		RawBufferSize:     100000,
		DecodedBufferSize: 100000,
		FrameBufferSize:   100000,
		ErrorBufferSize:   10000,
		DecoderWorkers:    4,
		SolverWorkers:     4,
	}
}

func NewPipeline(cfg PipelineConfig, solver *driftflux.DriftFluxSolver, log *logger.Logger,
	decodedChan chan *protocol.RawSensorData,
	frameChan chan *driftflux.HydrodynamicsFrame,
	errChan chan error) *Pipeline {

	ctx, cancel := context.WithCancel(context.Background())

	return &Pipeline{
		rawDataChan:    make(chan []byte, cfg.RawBufferSize),
		decodedChan:    decodedChan,
		hydroFrameChan: frameChan,
		errorChan:      errChan,
		decoder:        protocol.NewBitDecoder(),
		solver:         solver,
		logger:         log,
		ctx:            ctx,
		cancel:         cancel,
		decoderCount:   cfg.DecoderWorkers,
		solverCount:    cfg.SolverWorkers,
	}
}

func (p *Pipeline) Start() {
	if !p.running.CompareAndSwap(false, true) {
		return
	}

	p.logger.Info("Starting data pipeline with %d decoder workers and %d solver workers",
		p.decoderCount, p.solverCount)

	for i := 0; i < p.decoderCount; i++ {
		p.wg.Add(1)
		go p.decoderWorker(i)
	}

	for i := 0; i < p.solverCount; i++ {
		p.wg.Add(1)
		go p.solverWorker(i)
	}

	p.wg.Add(1)
	go p.errorHandler()

	p.logger.Info("Data pipeline started successfully")
}

func (p *Pipeline) decoderWorker(id int) {
	defer p.wg.Done()
	p.logger.Debug("Decoder worker %d started", id)

	for p.running.Load() {
		select {
		case <-p.ctx.Done():
			p.logger.Debug("Decoder worker %d stopping", id)
			return
		case rawData, ok := <-p.rawDataChan:
			if !ok {
				return
			}

			frame, err := p.decoder.DecodeModbusRTU(rawData)
			if err != nil {
				select {
				case p.errorChan <- err:
				default:
				}
				continue
			}

			if !frame.IsValid {
				continue
			}

			sensorData, err := p.decoder.ExtractSensorData(frame)
			if err != nil {
				select {
				case p.errorChan <- err:
				default:
				}
				continue
			}

			select {
			case p.decodedChan <- sensorData:
			default:
				p.logger.Warn("Decoded data channel full, dropping data")
			}
		}
	}
}

func (p *Pipeline) solverWorker(id int) {
	defer p.wg.Done()
	p.logger.Debug("Solver worker %d started", id)

	for p.running.Load() {
		select {
		case <-p.ctx.Done():
			p.logger.Debug("Solver worker %d stopping", id)
			return
		case sensorData, ok := <-p.decodedChan:
			if !ok {
				return
			}

			frame, err := p.solver.Solve(sensorData)
			if err != nil {
				select {
				case p.errorChan <- err:
				default:
				}
				continue
			}

			select {
			case p.hydroFrameChan <- frame:
			default:
				p.logger.Warn("Hydrodynamics frame channel full, dropping frame")
			}
		}
	}
}

func (p *Pipeline) errorHandler() {
	defer p.wg.Done()

	for p.running.Load() {
		select {
		case <-p.ctx.Done():
			return
		case err, ok := <-p.errorChan:
			if !ok {
				return
			}
			p.logger.Debug("Pipeline error: %v", err)
		}
	}
}

func (p *Pipeline) Input() chan<- []byte {
	return p.rawDataChan
}

func (p *Pipeline) Output() <-chan *driftflux.HydrodynamicsFrame {
	return p.hydroFrameChan
}

func (p *Pipeline) Errors() <-chan error {
	return p.errorChan
}

func (p *Pipeline) Stop() {
	if !p.running.CompareAndSwap(true, false) {
		return
	}

	p.cancel()

	close(p.rawDataChan)

	p.wg.Wait()
	p.logger.Info("Data pipeline stopped")
}

func (p *Pipeline) GetStats() (rawLen, decodedLen, frameLen int) {
	return len(p.rawDataChan), len(p.decodedChan), len(p.hydroFrameChan)
}

func (p *Pipeline) GetDecoderStats() (total, invalid, noiseFiltered uint64) {
	return p.decoder.GetStats()
}
