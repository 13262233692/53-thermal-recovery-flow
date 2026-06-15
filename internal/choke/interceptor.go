package choke

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"thermal-recovery-flow/internal/driftflux"
	"thermal-recovery-flow/internal/safety"
	"thermal-recovery-flow/pkg/logger"
)

type ChokeInterceptorConfig struct {
	ControllerConfig SafetyControllerConfig
	SenderConfig     OverrideSenderConfig
	DownstreamPressure float64
	InitialOpening    float64
	Enabled           bool
}

func DefaultChokeInterceptorConfig() ChokeInterceptorConfig {
	return ChokeInterceptorConfig{
		ControllerConfig:   DefaultSafetyControllerConfig(),
		SenderConfig:       DefaultOverrideSenderConfig(),
		DownstreamPressure: 101325.0,
		InitialOpening:     1.0,
		Enabled:            true,
	}
}

type ChokeFrame struct {
	Timestamp          time.Time
	SafetyAssessment   SafetyAssessment
	ChokeResult        ChokeValveResult
	CurrentOpening     float64
	OverrideSent       bool
	OverridePacket     *OverridePacket
	ThreatLevel        ThreatLevel
	PressureDerivative float64
	PredictedPressure  float64
	TimeToBreach       time.Duration
}

type ChokeInterceptor struct {
	config        ChokeInterceptorConfig
	controller    *SafetyController
	sender        *OverrideSender
	logger        *logger.Logger
	inputChan     <-chan *driftflux.HydrodynamicsFrame
	outputChan    chan<- *ChokeFrame
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	running       atomic.Bool
	currentOpening float64
	overrideCount uint64
	lockdownCount uint64
	totalFrames   uint64
}

func NewChokeInterceptor(
	config ChokeInterceptorConfig,
	input <-chan *driftflux.HydrodynamicsFrame,
	output chan<- *ChokeFrame,
	log *logger.Logger,
) *ChokeInterceptor {
	ctx, cancel := context.WithCancel(context.Background())
	return &ChokeInterceptor{
		config:        config,
		controller:    NewSafetyController(config.ControllerConfig),
		sender:        NewOverrideSender(config.SenderConfig, log),
		logger:        log,
		inputChan:     input,
		outputChan:    output,
		ctx:           ctx,
		cancel:        cancel,
		currentOpening: config.InitialOpening,
	}
}

func (ci *ChokeInterceptor) Start() {
	if !ci.running.CompareAndSwap(false, true) {
		return
	}

	if ci.config.Enabled {
		ci.sender.Start()
	}

	ci.wg.Add(1)
	safety.SafeGoWG(ci.logger, "choke.interceptor",
		func() { ci.wg.Done() },
		func() { ci.processLoop() })

	ci.logger.Info("Choke Interceptor started (safety limit: %.1f MPa, enabled: %v)",
		ci.config.ControllerConfig.SafetyPressureLimit/1e6, ci.config.Enabled)
}

func (ci *ChokeInterceptor) Stop() {
	if !ci.running.CompareAndSwap(true, false) {
		return
	}

	ci.logger.Info("Choke Interceptor stopping...")
	ci.cancel()

	if ci.config.Enabled {
		ci.sender.Stop()
	}

	done := make(chan struct{})
	go func() {
		ci.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		ci.logger.Warn("Choke Interceptor shutdown timed out")
	}

	ci.logger.Info("Choke Interceptor stopped (frames: %d, overrides: %d, lockdowns: %d)",
		atomic.LoadUint64(&ci.totalFrames),
		atomic.LoadUint64(&ci.overrideCount),
		atomic.LoadUint64(&ci.lockdownCount))
}

func (ci *ChokeInterceptor) processLoop() {
	defer func() {
		safety.SafeRecover(ci.logger, "choke.interceptor")
	}()

	for ci.running.Load() {
		select {
		case <-ci.ctx.Done():
			return
		case frame, ok := <-ci.inputChan:
			if !ok {
				return
			}
			if frame == nil {
				continue
			}
			ci.processFrame(frame)
		}
	}
}

func (ci *ChokeInterceptor) processFrame(frame *driftflux.HydrodynamicsFrame) {
	atomic.AddUint64(&ci.totalFrames, 1)

	upstreamP := frame.Pressure
	upstreamT := frame.Temperature
	vaporFlux := frame.VaporVelocity * frame.VaporDensity * frame.VoidFraction
	downstreamP := ci.config.DownstreamPressure

	assessment := ci.controller.Assess(
		upstreamP, upstreamT, vaporFlux,
		downstreamP, ci.currentOpening)

	chokeResult := SolveChokeFlow(
		ci.config.ControllerConfig.ValveConfig,
		upstreamP, upstreamT,
		assessment.ChokeResult.UpstreamRho,
		downstreamP, ci.currentOpening)

	chokeFrame := &ChokeFrame{
		Timestamp:          frame.Timestamp,
		SafetyAssessment:   assessment,
		ChokeResult:        chokeResult,
		CurrentOpening:     ci.currentOpening,
		OverrideSent:       false,
		ThreatLevel:        assessment.ThreatLevel,
		PressureDerivative: assessment.PressureDerivative,
		PredictedPressure:  assessment.PredictedPressure,
		TimeToBreach:       assessment.TimeToBreach,
	}

	if assessment.RequiresOverride && ci.config.Enabled {
		var packet OverridePacket
		if assessment.LockdownRequired {
			packet = BuildLockdownOverride(
				ci.config.ControllerConfig.OverrideAddress,
				ci.config.ControllerConfig.OverrideRegister,
				assessment)
			atomic.AddUint64(&ci.lockdownCount, 1)
			ci.logger.Error("LOCKDOWN OVERRIDE: P=%.2f MPa, dP/dt=%.2e Pa/s, predicted=%.2f MPa, tBreach=%v",
				upstreamP/1e6, assessment.PressureDerivative,
				assessment.PredictedPressure/1e6, assessment.TimeToBreach)
		} else {
			packet = BuildEmergencyOverride(
				ci.config.ControllerConfig.OverrideAddress,
				ci.config.ControllerConfig.OverrideRegister,
				assessment.RequiredOpening,
				assessment)
			atomic.AddUint64(&ci.overrideCount, 1)
			ci.logger.Warn("SAFETY OVERRIDE: P=%.2f MPa, dP/dt=%.2e Pa/s, opening=%.3f->%.3f, tBreach=%v",
				upstreamP/1e6, assessment.PressureDerivative,
				ci.currentOpening, assessment.RequiredOpening,
				assessment.TimeToBreach)
		}

		if assessment.ThreatLevel >= ThreatCritical {
			result := ci.sender.EmergencySend(packet)
			chokeFrame.OverrideSent = result.Success
			if result.Success {
				ci.logger.Info("Emergency override sent in %v (retries: %d)",
					result.SendTime, result.Retries)
			} else {
				ci.logger.Error("Emergency override FAILED after %d retries: %v",
					result.Retries, result.Error)
			}
		} else {
			ci.sender.Send(packet)
			chokeFrame.OverrideSent = true
		}

		chokeFrame.OverridePacket = &packet
		ci.currentOpening = assessment.RequiredOpening
	}

	if ci.outputChan != nil {
		select {
		case ci.outputChan <- chokeFrame:
		case <-ci.ctx.Done():
			return
		default:
		}
	}
}

func (ci *ChokeInterceptor) GetCurrentOpening() float64 {
	return ci.currentOpening
}

func (ci *ChokeInterceptor) GetStats() (frames, overrides, lockdowns uint64) {
	return atomic.LoadUint64(&ci.totalFrames),
		atomic.LoadUint64(&ci.overrideCount),
		atomic.LoadUint64(&ci.lockdownCount)
}

func (ci *ChokeInterceptor) SetDownstreamPressure(p float64) {
	ci.config.DownstreamPressure = p
}

func (ci *ChokeInterceptor) SetCurrentOpening(opening float64) {
	if opening < 0 {
		opening = 0
	}
	if opening > 1.0 {
		opening = 1.0
	}
	ci.currentOpening = opening
}
