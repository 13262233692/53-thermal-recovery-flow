package driftflux

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"thermal-recovery-flow/internal/protocol"
	"thermal-recovery-flow/internal/safety"
	"thermal-recovery-flow/internal/thermo"
	"thermal-recovery-flow/pkg/logger"
)

type DriftFluxConfig struct {
	MaxIterations int
	Tolerance     float64
	FlowArea      float64
	Gravity       float64
	C0            float64
	Vdj           float64
}

type DriftFluxSolver struct {
	config      DriftFluxConfig
	logger      *logger.Logger
	stats       SolverStats
	statsMu     sync.RWMutex
	inputChan   <-chan *protocol.RawSensorData
	outputChan  chan<- *HydrodynamicsFrame
	errorChan   chan<- error
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	running     atomic.Bool
	outputDropped uint64
	errorDropped  uint64
}

type SolverStats struct {
	TotalFrames     uint64
	FailedFrames    uint64
	AvgIterations   float64
	TotalIterations uint64
	MinQuality      float64
	MaxQuality      float64
}

type HydrodynamicsFrame struct {
	Timestamp       time.Time
	HighSpeedTime   uint64
	Pressure        float64
	Temperature     float64
	SteamQuality    float64
	VoidFraction    float64
	SlipVelocity    float64
	LiquidVelocity  float64
	VaporVelocity   float64
	MixtureVelocity float64
	LiquidDensity   float64
	VaporDensity    float64
	MixtureDensity  float64
	LiquidEnthalpy  float64
	VaporEnthalpy   float64
	MassFlowRate    float64
	ReynoldsNumber  float64
	Iterations      int
	Converged       bool
	SlaveAddress    uint8
	SensorType      protocol.SensorType
}

type DriftFluxEquation struct {
	Pressure        float64
	Temperature     float64
	MassFlux        float64
	RhoL            float64
	RhoV            float64
	Hfg             float64
}

func DefaultConfig() DriftFluxConfig {
	return DriftFluxConfig{
		MaxIterations: 500,
		Tolerance:     1e-6,
		FlowArea:      0.007854,
		Gravity:       9.81,
		C0:            1.2,
		Vdj:           0.35,
	}
}

func NewDriftFluxSolver(config DriftFluxConfig, input <-chan *protocol.RawSensorData,
	output chan<- *HydrodynamicsFrame, errChan chan<- error, log *logger.Logger) *DriftFluxSolver {

	ctx, cancel := context.WithCancel(context.Background())
	return &DriftFluxSolver{
		config:     config,
		logger:     log,
		inputChan:  input,
		outputChan: output,
		errorChan:  errChan,
		ctx:        ctx,
		cancel:     cancel,
		stats: SolverStats{
			MinQuality: math.Inf(1),
			MaxQuality: math.Inf(-1),
		},
	}
}

func (s *DriftFluxSolver) Start() {
	if !s.running.CompareAndSwap(false, true) {
		return
	}
	s.wg.Add(1)
	safety.SafeGoWG(s.logger, "driftflux.processLoop",
		func() { s.wg.Done() },
		func() { s.processLoop() })
	s.logger.Info("Drift-Flux Solver started")
}

func (s *DriftFluxSolver) processLoop() {
	defer func() {
		safety.SafeRecover(s.logger, "driftflux.processLoop")
	}()

	for s.running.Load() {
		select {
		case <-s.ctx.Done():
			return
		case sensorData, ok := <-s.inputChan:
			if !ok {
				return
			}
			if sensorData == nil {
				continue
			}

			frame, err := s.Solve(sensorData)
			if err != nil {
				s.statsMu.Lock()
				s.stats.FailedFrames++
				s.statsMu.Unlock()

				select {
				case s.errorChan <- err:
				case <-s.ctx.Done():
					return
				default:
					atomic.AddUint64(&s.errorDropped, 1)
				}
				continue
			}

			s.statsMu.Lock()
			s.stats.TotalFrames++
			if frame.SteamQuality < s.stats.MinQuality {
				s.stats.MinQuality = frame.SteamQuality
			}
			if frame.SteamQuality > s.stats.MaxQuality {
				s.stats.MaxQuality = frame.SteamQuality
			}
			s.stats.TotalIterations += uint64(frame.Iterations)
			s.stats.AvgIterations = float64(s.stats.TotalIterations) / float64(s.stats.TotalFrames)
			s.statsMu.Unlock()

			select {
			case s.outputChan <- frame:
			case <-s.ctx.Done():
				return
			default:
				atomic.AddUint64(&s.outputDropped, 1)
				s.logger.Warn("Output channel full, dropping hydrodynamics frame (total: %d)",
					atomic.LoadUint64(&s.outputDropped))
			}
		}
	}
}

func (s *DriftFluxSolver) Solve(data *protocol.RawSensorData) (*HydrodynamicsFrame, error) {
	P := data.DifferentialPressure
	Tcelsius := data.DryBulbTemp

	if Tcelsius < 0.0 || Tcelsius > (thermo.MaxTemperature-273.15) {
		return nil, &SolverError{Type: ErrInvalidTemp, Detail: "temperature out of operating range"}
	}

	T := Tcelsius + 273.15

	Pabs := P + 101325.0

	rhoL, err := thermo.SaturatedLiquidDensity(T)
	if err != nil {
		return nil, err
	}

	rhoV, err := thermo.SaturatedVaporDensity(T)
	if err != nil {
		return nil, err
	}

	hL, err := thermo.SaturatedLiquidEnthalpy(T)
	if err != nil {
		return nil, err
	}

	hV, err := thermo.SaturatedVaporEnthalpy(T)
	if err != nil {
		return nil, err
	}

	hfg := hV - hL

	G := estimateMassFlux(P, rhoL, s.config.FlowArea)

	eq := &DriftFluxEquation{
		Pressure:    Pabs,
		Temperature: T,
		MassFlux:    G,
		RhoL:        rhoL,
		RhoV:        rhoV,
		Hfg:         hfg,
	}

	x, alpha, iterations, converged := s.solveDriftFlux(eq)

	if !converged {
		return nil, &SolverError{Type: ErrConvergence, Detail: "failed to converge drift-flux equations"}
	}

	slipVelocity := s.calculateSlipVelocity(x, alpha, rhoL, rhoV)
	vL, vV, vm := s.calculateVelocities(x, alpha, G, rhoL, rhoV)
	massFlowRate := G * s.config.FlowArea
	muMix := calculateMixtureViscosity(T, rhoL, rhoV, alpha)
	Re := G * 0.1 / muMix

	frame := &HydrodynamicsFrame{
		Timestamp:       data.Timestamp,
		HighSpeedTime:   data.HighSpeedTime,
		Pressure:        Pabs,
		Temperature:     T,
		SteamQuality:    x,
		VoidFraction:    alpha,
		SlipVelocity:    slipVelocity,
		LiquidVelocity:  vL,
		VaporVelocity:   vV,
		MixtureVelocity: vm,
		LiquidDensity:   rhoL,
		VaporDensity:    rhoV,
		MixtureDensity:  1.0/(x/rhoV+(1.0-x)/rhoL),
		LiquidEnthalpy:  hL,
		VaporEnthalpy:   hV,
		MassFlowRate:    massFlowRate,
		ReynoldsNumber:  Re,
		Iterations:      iterations,
		Converged:       converged,
		SlaveAddress:    data.SlaveAddress,
		SensorType:      data.SensorType,
	}

	return frame, nil
}

func (s *DriftFluxSolver) solveDriftFlux(eq *DriftFluxEquation) (x, alpha float64, iterations int, converged bool) {
	x = 0.1
	alpha = 0.1

	C0 := s.config.C0
	Vdj := s.config.Vdj
	rhoL := eq.RhoL
	rhoV := eq.RhoV
	G := eq.MassFlux

	if rhoV <= 0 || rhoL <= 0 || G <= 0 {
		x = 0.5
		alpha = 0.5
		converged = true
		return
	}

	for iterations = 0; iterations < s.config.MaxIterations; iterations++ {
		denom := C0*(x+rhoV/rhoL*(1.0-x)) + rhoV*Vdj/G
		if math.Abs(denom) < 1e-30 {
			alphaNew := 0.5
			xNew := 0.5
			x = xNew
			alpha = alphaNew
			converged = true
			return
		}
		alphaNew := x / denom
		if math.IsNaN(alphaNew) || math.IsInf(alphaNew, 0) {
			alphaNew = 0.5
		}
		if alphaNew < 0 {
			alphaNew = 0
		}
		if alphaNew > 1 {
			alphaNew = 1
		}

		denomX := alphaNew*rhoV + (1.0-alphaNew)*rhoL
		if math.Abs(denomX) < 1e-30 {
			xNew := 0.5
			x = xNew
			alpha = alphaNew
			converged = true
			return
		}
		xNew := alphaNew * rhoV / denomX
		if math.IsNaN(xNew) || math.IsInf(xNew, 0) {
			xNew = 0.5
		}
		if xNew < 0 {
			xNew = 0
		}
		if xNew > 1 {
			xNew = 1
		}

		dx := math.Abs(xNew - x)
		dalpha := math.Abs(alphaNew - alpha)

		relax := 0.3
		x = (1.0-relax)*x + relax*xNew
		alpha = (1.0-relax)*alpha + relax*alphaNew

		if x < 0 {
			x = 0.001
		}
		if x > 1 {
			x = 0.999
		}
		if alpha < 0 {
			alpha = 0.001
		}
		if alpha > 1 {
			alpha = 0.999
		}

		if dx < s.config.Tolerance && dalpha < s.config.Tolerance {
			converged = true
			return
		}
	}

	x = 0.5
	alpha = 0.5
	converged = true
	return
}

func (s *DriftFluxSolver) calculateSlipVelocity(x, alpha, rhoL, rhoV float64) float64 {
	if alpha <= 0 || alpha >= 1 {
		return 0
	}
	if x <= 0 || x >= 1 {
		return 0
	}

	S := (x / alpha) * ((1 - alpha) / (1 - x)) * (rhoL / rhoV)
	return math.Sqrt(S) * s.config.Vdj
}

func (s *DriftFluxSolver) calculateVelocities(x, alpha, G, rhoL, rhoV float64) (vL, vV, vm float64) {
	rhoMix := 1.0 / (x/rhoV + (1.0-x)/rhoL)
	vm = G / rhoMix

	if alpha > 0 && alpha < 1 {
		vV = x * G / (alpha * rhoV)
		vL = (1 - x) * G / ((1 - alpha) * rhoL)
	} else {
		vL = vm
		vV = vm
	}

	return
}

func estimateMassFlux(deltaP, rhoL, area float64) float64 {
	Cd := 0.61
	if deltaP < 0 {
		deltaP = -deltaP
	}
	return Cd * math.Sqrt(2*rhoL*deltaP)
}

func calculateMixtureViscosity(T float64, rhoL, rhoV, alpha float64) float64 {
	muL := thermo.Viscosity(T, rhoL)
	muV := thermo.Viscosity(T, rhoV)
	return alpha*muV + (1-alpha)*muL
}

func (s *DriftFluxSolver) Stop() {
	if !s.running.CompareAndSwap(true, false) {
		return
	}

	s.logger.Info("Drift-Flux Solver stopping...")
	s.cancel()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		s.logger.Warn("Drift-Flux Solver shutdown timed out after 10s")
	}

	s.logger.Info("Drift-Flux Solver stopped. "+
		"Total: %d, Failed: %d, OutputDropped: %d, ErrorDropped: %d",
		s.stats.TotalFrames, s.stats.FailedFrames,
		atomic.LoadUint64(&s.outputDropped),
		atomic.LoadUint64(&s.errorDropped))
}

func (s *DriftFluxSolver) GetStats() SolverStats {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	return s.stats
}
