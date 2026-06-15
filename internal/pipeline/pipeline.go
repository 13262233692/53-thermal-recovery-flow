package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"thermal-recovery-flow/internal/driftflux"
	"thermal-recovery-flow/internal/protocol"
	"thermal-recovery-flow/internal/safety"
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
	stopped        atomic.Bool
	decoderCount   int
	solverCount    int
	decodedDropped uint64
	frameDropped   uint64
	errDropped     uint64
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
	p.stopped.Store(false)

	p.logger.Info("Starting data pipeline with %d decoder workers and %d solver workers",
		p.decoderCount, p.solverCount)

	for i := 0; i < p.decoderCount; i++ {
		p.wg.Add(1)
		workerID := i
		safety.SafeGoWG(p.logger, fmt.Sprintf("pipeline.decoderWorker[%d]", workerID),
			func() { p.wg.Done() },
			func() { p.decoderWorker(workerID) })
	}

	for i := 0; i < p.solverCount; i++ {
		p.wg.Add(1)
		workerID := i
		safety.SafeGoWG(p.logger, fmt.Sprintf("pipeline.solverWorker[%d]", workerID),
			func() { p.wg.Done() },
			func() { p.solverWorker(workerID) })
	}

	p.wg.Add(1)
	safety.SafeGoWG(p.logger, "pipeline.errorHandler",
		func() { p.wg.Done() },
		func() { p.errorHandler() })

	p.logger.Info("Data pipeline started successfully")
}

func (p *Pipeline) decoderWorker(id int) {
	defer func() {
		safety.SafeRecover(p.logger, fmt.Sprintf("pipeline.decoderWorker[%d]", id))
	}()

	p.logger.Debug("Decoder worker %d started", id)

	for p.running.Load() {
		select {
		case <-p.ctx.Done():
			p.logger.Debug("Decoder worker %d stopping (ctx done)", id)
			return
		case rawData, ok := <-p.rawDataChan:
			if !ok {
				p.logger.Debug("Decoder worker %d stopping (channel closed)", id)
				return
			}
			if len(rawData) == 0 {
				continue
			}

			frame, err := p.decoder.DecodeModbusRTU(rawData)
			if err != nil {
				p.sendError(err)
				continue
			}

			if !frame.IsValid {
				continue
			}

			sensorData, err := p.decoder.ExtractSensorData(frame)
			if err != nil {
				p.sendError(err)
				continue
			}

			select {
			case p.decodedChan <- sensorData:
			case <-p.ctx.Done():
				return
			default:
				atomic.AddUint64(&p.decodedDropped, 1)
				p.logger.Warn("Decoded data channel full, dropping data (total dropped: %d)",
					atomic.LoadUint64(&p.decodedDropped))
			}
		}
	}
}

func (p *Pipeline) solverWorker(id int) {
	defer func() {
		safety.SafeRecover(p.logger, fmt.Sprintf("pipeline.solverWorker[%d]", id))
	}()

	p.logger.Debug("Solver worker %d started", id)

	for p.running.Load() {
		select {
		case <-p.ctx.Done():
			p.logger.Debug("Solver worker %d stopping (ctx done)", id)
			return
		case sensorData, ok := <-p.decodedChan:
			if !ok {
				p.logger.Debug("Solver worker %d stopping (channel closed)", id)
				return
			}
			if sensorData == nil {
				continue
			}

			frame, err := p.solver.Solve(sensorData)
			if err != nil {
				p.sendError(err)
				continue
			}

			select {
			case p.hydroFrameChan <- frame:
			case <-p.ctx.Done():
				return
			default:
				atomic.AddUint64(&p.frameDropped, 1)
				p.logger.Warn("Hydrodynamics frame channel full, dropping frame (total dropped: %d)",
					atomic.LoadUint64(&p.frameDropped))
			}
		}
	}
}

func (p *Pipeline) errorHandler() {
	defer func() {
		safety.SafeRecover(p.logger, "pipeline.errorHandler")
	}()

	for p.running.Load() {
		select {
		case <-p.ctx.Done():
			return
		case err, ok := <-p.errorChan:
			if !ok {
				return
			}
			if err != nil {
				p.logger.Debug("Pipeline error: %v", err)
			}
		}
	}
}

func (p *Pipeline) sendError(err error) {
	if err == nil {
		return
	}
	select {
	case p.errorChan <- err:
	default:
		atomic.AddUint64(&p.errDropped, 1)
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
	if !p.stopped.CompareAndSwap(false, true) {
		return
	}

	p.logger.Info("Data pipeline stopping...")

	p.cancel()

	defer func() {
		if r := recover(); r != nil {
			p.logger.Debug("Recovered during channel close: %v", r)
		}
	}()
	close(p.rawDataChan)

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		p.logger.Warn("Data pipeline shutdown timed out after 15s")
	}

	total, invalid, noise := p.decoder.GetStats()
	p.logger.Info("Data pipeline stopped. "+
		"decodedDropped=%d, frameDropped=%d, errDropped=%d, "+
		"totalFrames=%d, invalidFrames=%d, noiseFiltered=%d",
		atomic.LoadUint64(&p.decodedDropped),
		atomic.LoadUint64(&p.frameDropped),
		atomic.LoadUint64(&p.errDropped),
		total, invalid, noise)
}

func (p *Pipeline) GetStats() (rawLen, decodedLen, frameLen int) {
	return len(p.rawDataChan), len(p.decodedChan), len(p.hydroFrameChan)
}

func (p *Pipeline) GetDropStats() (decodedDropped, frameDropped, errDropped uint64) {
	return atomic.LoadUint64(&p.decodedDropped),
		atomic.LoadUint64(&p.frameDropped),
		atomic.LoadUint64(&p.errDropped)
}

func (p *Pipeline) GetDecoderStats() (total, invalid, noiseFiltered uint64) {
	return p.decoder.GetStats()
}
