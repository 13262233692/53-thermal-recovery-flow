package choke

import (
	"math"
	"sync"
	"time"
)

type ThreatLevel int

const (
	ThreatNormal ThreatLevel = iota
	ThreatElevated
	ThreatCritical
	ThreatImminent
)

type SafetyAssessment struct {
	ThreatLevel          ThreatLevel
	CurrentPressure      float64
	PressureDerivative   float64
	SecondDerivative     float64
	PredictedPressure    float64
	TimeToBreach         time.Duration
	RequiredOpening      float64
	SafetyMargin         float64
	IsNonlinearSurge     bool
	RequiresOverride     bool
	LockdownRequired     bool
	ChokeResult         ChokeValveResult
}

type PressureSample struct {
	Timestamp time.Time
	Pressure  float64
	Temperature float64
	VaporFlux float64
}

type SafetyControllerConfig struct {
	SafetyPressureLimit  float64
	WarningPressureRatio float64
	PredictionHorizon    time.Duration
	MaxDerivative        float64
	SurgeThreshold       float64
	LockdownOpening      float64
	MinSamplesForDeriv   int
	ValveConfig          ChokeValveConfig
	OverrideAddress      uint8
	OverrideRegister     uint16
}

func DefaultSafetyControllerConfig() SafetyControllerConfig {
	return SafetyControllerConfig{
		SafetyPressureLimit:  35.0e6,
		WarningPressureRatio: 0.85,
		PredictionHorizon:    100 * time.Millisecond,
		MaxDerivative:        1.0e8,
		SurgeThreshold:       5.0e7,
		LockdownOpening:      0.0,
		MinSamplesForDeriv:   3,
		ValveConfig:          DefaultChokeValveConfig(),
		OverrideAddress:      0x01,
		OverrideRegister:     0x0100,
	}
}

type SafetyController struct {
	config        SafetyControllerConfig
	samples       []PressureSample
	mu            sync.RWMutex
	lastAssessment SafetyAssessment
	overrideCount uint64
	totalAssess   uint64
}

func NewSafetyController(config SafetyControllerConfig) *SafetyController {
	return &SafetyController{
		config:  config,
		samples: make([]PressureSample, 0, 64),
	}
}

func (c *SafetyController) Assess(pressure, temperature, vaporFlux, downstreamP, currentOpening float64) SafetyAssessment {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.totalAssess++

	now := time.Now()
	sample := PressureSample{
		Timestamp:  now,
		Pressure:   pressure,
		Temperature: temperature,
		VaporFlux:  vaporFlux,
	}
	c.samples = append(c.samples, sample)

	const maxSamples = 64
	if len(c.samples) > maxSamples {
		c.samples = c.samples[len(c.samples)-maxSamples:]
	}

	assessment := SafetyAssessment{
		CurrentPressure: pressure,
		RequiredOpening: currentOpening,
	}

	dPdt, d2Pdt2 := c.computeDerivatives()
	assessment.PressureDerivative = dPdt
	assessment.SecondDerivative = d2Pdt2

	safetyLimit := c.config.SafetyPressureLimit
	warningLimit := safetyLimit * c.config.WarningPressureRatio
	assessment.SafetyMargin = (safetyLimit - pressure) / safetyLimit

	assessment.IsNonlinearSurge = c.detectNonlinearSurge(dPdt, d2Pdt2)

	predictedP := c.predictPressure(pressure, dPdt, d2Pdt2, c.config.PredictionHorizon)
	assessment.PredictedPressure = predictedP

	if predictedP >= safetyLimit {
		horizonSec := c.config.PredictionHorizon.Seconds()
		if dPdt > 0 {
			timeToBreach := (safetyLimit - pressure) / dPdt
			if timeToBreach < 0 {
				timeToBreach = 0
			}
			assessment.TimeToBreach = time.Duration(timeToBreach * float64(time.Second))
		} else {
			assessment.TimeToBreach = time.Duration(horizonSec * float64(time.Second))
		}
	} else if dPdt > 0 {
		timeToBreach := (safetyLimit - pressure) / dPdt
		assessment.TimeToBreach = time.Duration(timeToBreach * float64(time.Second))
	} else {
		assessment.TimeToBreach = time.Duration(math.MaxInt64)
	}

	upstreamRho := 0.0
	if temperature > 0 {
		upstreamRho = pressure / (RGas * temperature)
		if upstreamRho < 0 {
			upstreamRho = 0
		}
	}

	chokeResult := SolveChokeFlow(c.config.ValveConfig, pressure, temperature, upstreamRho, downstreamP, currentOpening)
	assessment.ChokeResult = chokeResult

	switch {
	case pressure >= safetyLimit || (assessment.IsNonlinearSurge && predictedP >= safetyLimit):
		assessment.ThreatLevel = ThreatImminent
		assessment.LockdownRequired = true
		assessment.RequiresOverride = true
		assessment.RequiredOpening = c.config.LockdownOpening

	case assessment.IsNonlinearSurge && assessment.TimeToBreach <= c.config.PredictionHorizon:
		assessment.ThreatLevel = ThreatCritical
		assessment.RequiresOverride = true
		assessment.RequiredOpening = ComputeRequiredOpening(
			c.config.ValveConfig, pressure, temperature, upstreamRho,
			downstreamP, vaporFlux*0.3)

	case pressure >= warningLimit || (dPdt > c.config.MaxDerivative):
		assessment.ThreatLevel = ThreatElevated
		assessment.RequiresOverride = false
		assessment.RequiredOpening = ComputeRequiredOpening(
			c.config.ValveConfig, pressure, temperature, upstreamRho,
			downstreamP, vaporFlux*0.6)

	default:
		assessment.ThreatLevel = ThreatNormal
		assessment.RequiresOverride = false
		assessment.RequiredOpening = currentOpening
	}

	if assessment.RequiresOverride {
		c.overrideCount++
	}

	c.lastAssessment = assessment
	return assessment
}

func (c *SafetyController) computeDerivatives() (dPdt, d2Pdt2 float64) {
	n := len(c.samples)
	if n < c.config.MinSamplesForDeriv {
		return 0, 0
	}

	dPdt = c.computeFirstDerivative()
	d2Pdt2 = c.computeSecondDerivative()

	return dPdt, d2Pdt2
}

func (c *SafetyController) computeFirstDerivative() float64 {
	n := len(c.samples)
	if n < 2 {
		return 0
	}

	windowSize := min(n, 10)
	start := n - windowSize

	var sumP float64
	var sumT float64
	var sumPT float64
	var sumT2 float64

	for i := start; i < n; i++ {
		t := c.samples[i].Timestamp.Sub(c.samples[start].Timestamp).Seconds()
		p := c.samples[i].Pressure
		sumP += p
		sumT += t
		sumPT += p * t
		sumT2 += t * t
	}

	count := float64(windowSize)
	denom := count*sumT2 - sumT*sumT
	if math.Abs(denom) < 1e-30 {
		return 0
	}

	slope := (count*sumPT - sumP*sumT) / denom
	return slope
}

func (c *SafetyController) computeSecondDerivative() float64 {
	n := len(c.samples)
	if n < 3 {
		return 0
	}

	windowSize := min(n, 10)
	if windowSize < 3 {
		return 0
	}
	start := n - windowSize

	firstDerivs := make([]float64, 0, windowSize-1)
	for i := start + 1; i < n; i++ {
		dt := c.samples[i].Timestamp.Sub(c.samples[i-1].Timestamp).Seconds()
		if dt <= 0 {
			continue
		}
		dp := c.samples[i].Pressure - c.samples[i-1].Pressure
		firstDerivs = append(firstDerivs, dp/dt)
	}

	if len(firstDerivs) < 2 {
		return 0
	}

	dStart := 0
	dEnd := len(firstDerivs)

	var sumD float64
	var sumT float64
	var sumDT float64
	var sumT2 float64

	for i := dStart; i < dEnd; i++ {
		t := float64(i)
		d := firstDerivs[i]
		sumD += d
		sumT += t
		sumDT += d * t
		sumT2 += t * t
	}

	count := float64(dEnd - dStart)
	denom := count*sumT2 - sumT*sumT
	if math.Abs(denom) < 1e-30 {
		return 0
	}

	slope := (count*sumDT - sumD*sumT) / denom
	return slope
}

func (c *SafetyController) detectNonlinearSurge(dPdt, d2Pdt2 float64) bool {
	if dPdt <= 0 {
		return false
	}

	if dPdt > c.config.SurgeThreshold {
		return true
	}

	if d2Pdt2 > 0 && dPdt > c.config.SurgeThreshold*0.3 {
		accelerationRatio := d2Pdt2 / dPdt
		if accelerationRatio > 10.0 {
			return true
		}
	}

	return false
}

func (c *SafetyController) predictPressure(currentP, dPdt, d2Pdt2 float64, horizon time.Duration) float64 {
	t := horizon.Seconds()
	predicted := currentP + dPdt*t + 0.5*d2Pdt2*t*t

	if predicted < 0 {
		predicted = 0
	}

	return predicted
}

func (c *SafetyController) GetStats() (totalAssess, overrideCount uint64, lastThreat ThreatLevel) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.totalAssess, c.overrideCount, c.lastAssessment.ThreatLevel
}

func (c *SafetyController) LastAssessment() SafetyAssessment {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastAssessment
}

func (c *SafetyController) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples = c.samples[:0]
	c.overrideCount = 0
	c.totalAssess = 0
}
