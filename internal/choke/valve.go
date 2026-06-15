package choke

import (
	"math"
)

const (
	GammaSteam        = 1.3
	RGas              = 461.5
	CdDefault         = 0.61
	MinOpening        = 0.001
	MaxOpening        = 1.0
	LockdownOpening   = 0.0
)

type FlowState int

const (
	FlowSubsonic FlowState = iota
	FlowChoked
	FlowUnknown
)

type ChokeValveConfig struct {
	Cd             float64
	MaxArea        float64
	Gamma          float64
	SafetyPressure float64
}

type ChokeValveResult struct {
	MassFlow       float64
	Velocity       float64
	FlowState      FlowState
	PressureRatio  float64
	CriticalRatio  float64
	ValveOpening   float64
	EffectiveArea  float64
	UpstreamP      float64
	DownstreamP    float64
	UpstreamT      float64
	UpstreamRho    float64
	IsChoked       bool
	SonicVelocity  float64
	MachNumber     float64
}

func DefaultChokeValveConfig() ChokeValveConfig {
	return ChokeValveConfig{
		Cd:             CdDefault,
		MaxArea:        0.007854,
		Gamma:          GammaSteam,
		SafetyPressure: 35.0e6,
	}
}

func CriticalPressureRatio(gamma float64) float64 {
	return math.Pow(2.0/(gamma+1.0), gamma/(gamma-1.0))
}

func SonicVelocity(T float64, gamma float64) float64 {
	return math.Sqrt(gamma * RGas * T)
}

func SolveChokeFlow(config ChokeValveConfig, upstreamP, upstreamT, upstreamRho, downstreamP, valveOpening float64) ChokeValveResult {
	result := ChokeValveResult{
		UpstreamP:   upstreamP,
		UpstreamT:   upstreamT,
		UpstreamRho: upstreamRho,
		DownstreamP: downstreamP,
	}

	if upstreamP <= 0 || upstreamT <= 0 || upstreamRho <= 0 {
		result.FlowState = FlowUnknown
		return result
	}

	gamma := config.Gamma
	Cd := config.Cd

	opening := valveOpening
	if opening < MinOpening {
		opening = MinOpening
	}
	if opening > MaxOpening {
		opening = MaxOpening
	}

	effectiveArea := config.MaxArea * opening * Cd
	result.ValveOpening = opening
	result.EffectiveArea = effectiveArea

	rCritical := CriticalPressureRatio(gamma)
	result.CriticalRatio = rCritical

	a := SonicVelocity(upstreamT, gamma)
	result.SonicVelocity = a

	pr := downstreamP / upstreamP
	if pr > 1.0 {
		pr = 1.0
	}
	if pr < 0.0 {
		pr = 0.0
	}
	result.PressureRatio = pr

	if pr <= rCritical {
		result.FlowState = FlowChoked
		result.IsChoked = true

		exponent := (gamma + 1.0) / (2.0 * (gamma - 1.0))
		massFlux := upstreamP * math.Sqrt(
			gamma/(RGas*upstreamT)) * math.Pow(2.0/(gamma+1.0), exponent)

		result.MassFlow = massFlux * effectiveArea
		result.Velocity = a
		result.MachNumber = 1.0
	} else {
		result.FlowState = FlowSubsonic
		result.IsChoked = false

		exponent := (gamma - 1.0) / gamma
		velocity := math.Sqrt(
			2.0 * gamma / (gamma - 1.0) * RGas * upstreamT *
				(1.0 - math.Pow(pr, exponent)))

		if math.IsNaN(velocity) || math.IsInf(velocity, 0) {
			velocity = 0
		}

		result.Velocity = velocity
		result.MachNumber = velocity / a

		rhoExit := upstreamRho * math.Pow(pr, 1.0/gamma)
		if rhoExit < 0 {
			rhoExit = 0
		}
		result.MassFlow = rhoExit * velocity * effectiveArea
	}

	if math.IsNaN(result.MassFlow) || math.IsInf(result.MassFlow, 0) {
		result.MassFlow = 0
	}
	if math.IsNaN(result.Velocity) || math.IsInf(result.Velocity, 0) {
		result.Velocity = 0
	}

	return result
}

func ComputeRequiredOpening(config ChokeValveConfig, upstreamP, upstreamT, upstreamRho, downstreamP, targetMassFlow float64) float64 {
	if targetMassFlow <= 0 || upstreamP <= 0 || upstreamT <= 0 || upstreamRho <= 0 {
		return MaxOpening
	}

	gamma := config.Gamma
	Cd := config.Cd
	rCritical := CriticalPressureRatio(gamma)
	pr := downstreamP / upstreamP
	if pr > 1.0 {
		pr = 1.0
	}

	var massFlux float64
	if pr <= rCritical {
		exponent := (gamma + 1.0) / (2.0 * (gamma - 1.0))
		massFlux = upstreamP * math.Sqrt(gamma/(RGas*upstreamT)) *
			math.Pow(2.0/(gamma+1.0), exponent)
	} else {
		exponent := (gamma - 1.0) / gamma
		velocity := math.Sqrt(
			2.0*gamma/(gamma-1.0)*RGas*upstreamT*
				(1.0-math.Pow(pr, exponent)))

		if math.IsNaN(velocity) || math.IsInf(velocity, 0) {
			return MaxOpening
		}

		rhoExit := upstreamRho * math.Pow(pr, 1.0/gamma)
		if rhoExit < 0 {
			rhoExit = 0
		}
		massFlux = rhoExit * velocity
	}

	if massFlux <= 0 || math.IsNaN(massFlux) || math.IsInf(massFlux, 0) {
		return MaxOpening
	}

	requiredArea := targetMassFlow / (massFlux * Cd)
	opening := requiredArea / config.MaxArea

	if opening < MinOpening {
		opening = MinOpening
	}
	if opening > MaxOpening {
		opening = MaxOpening
	}

	return opening
}
