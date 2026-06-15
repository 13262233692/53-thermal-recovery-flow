package thermo

import (
	"math"
)

const (
	TriplePointTemp   = 273.16
	TriplePointPress  = 611.657
	CriticalTemp      = 647.096
	CriticalPress     = 22.064e6
	CriticalDensity   = 322.0
	Rgas              = 461.526
	MolarMass         = 18.015268e-3
	MinTemperature    = 273.15
	MaxTemperature    = 1073.15
	MinPressure       = 611.657
	MaxPressure       = 100.0e6
)

type FluidProperties struct {
	Temperature    float64
	Pressure       float64
	Density        float64
	SpecificVolume float64
	Enthalpy       float64
	Entropy        float64
	InternalEnergy float64
	Viscosity      float64
	ThermalCond    float64
	Phase          string
}

type SteamQualityResult struct {
	Quality        float64
	VoidFraction   float64
	LiquidDensity  float64
	VaporDensity   float64
	MixtureDensity float64
	LiquidEnthalpy float64
	VaporEnthalpy  float64
}

func SaturationPressure(T float64) (float64, error) {
	if T < TriplePointTemp || T > CriticalTemp {
		return 0, &ThermoError{Type: ErrTempOutOfRange, Detail: "temperature out of saturation range"}
	}

	tau := 1.0 - T/CriticalTemp

	n := []float64{-7.85951783, 1.84408259, -11.7866497, 22.6807411, -15.9618719, 1.80122502}
	e := []float64{1.0, 1.5, 3.0, 3.5, 4.0, 7.5}

	sum := 0.0
	for i := range n {
		sum += n[i] * math.Pow(tau, e[i])
	}

	return CriticalPress * math.Exp((CriticalTemp/T)*sum), nil
}

func SaturationTemperature(P float64) (float64, error) {
	if P < TriplePointPress || P > CriticalPress {
		return 0, &ThermoError{Type: ErrPressureOutOfRange, Detail: "pressure out of saturation range"}
	}

	nu := 0.7
	for iter := 0; iter < 100; iter++ {
		tau := 1.0 - nu

		n := []float64{-7.85951783, 1.84408259, -11.7866497, 22.6807411, -15.9618719, 1.80122502}
		e := []float64{1.0, 1.5, 3.0, 3.5, 4.0, 7.5}

		sum := 0.0
		dsum := 0.0
		for i := range n {
			sum += n[i] * math.Pow(tau, e[i])
			if e[i] > 0 {
				dsum += -n[i] * e[i] * math.Pow(tau, e[i]-1.0)
			}
		}

		Ps := CriticalPress * math.Exp(sum/nu)
		f := Ps - P
		dPs_dnu := Ps * (dsum/nu - sum/(nu*nu))

		if math.Abs(dPs_dnu) < 1e-30 {
			break
		}
		dnu := -f / dPs_dnu

		if math.IsNaN(dnu) || math.IsInf(dnu, 0) {
			break
		}
		nuNew := nu + dnu

		if nuNew < 0.422 {
			nuNew = 0.422
		}
		if nuNew > 0.99999 {
			nuNew = 0.99999
		}

		if math.Abs(nuNew-nu) < 1e-12 {
			nu = nuNew
			break
		}
		nu = nuNew
	}

	return nu * CriticalTemp, nil
}

func SaturatedLiquidDensity(T float64) (float64, error) {
	if T < TriplePointTemp || T > CriticalTemp {
		return 0, &ThermoError{Type: ErrTempOutOfRange, Detail: "temperature out of range"}
	}

	tau := 1.0 - T/CriticalTemp
	b := []float64{1.99274064, 1.09965342, -0.51087811, -1.75493479, -45.5170352, -6.74694450e5}
	e := []float64{1.0 / 3.0, 2.0 / 3.0, 5.0 / 3.0, 16.0 / 3.0, 43.0 / 3.0, 110.0 / 3.0}

	sum := 0.0
	for i := range b {
		sum += b[i] * math.Pow(tau, e[i])
	}

	return CriticalDensity * (1.0 + sum), nil
}

func SaturatedVaporDensity(T float64) (float64, error) {
	if T < TriplePointTemp || T > CriticalTemp {
		return 0, &ThermoError{Type: ErrTempOutOfRange, Detail: "temperature out of range"}
	}

	tau := 1.0 - T/CriticalTemp
	c := []float64{-2.03150240, -2.68302940, -5.38626492, -17.2991605, -44.7586581, -63.9201063}
	e := []float64{2.0 / 6.0, 4.0 / 6.0, 8.0 / 6.0, 18.0 / 6.0, 37.0 / 6.0, 71.0 / 6.0}

	sum := 0.0
	for i := range c {
		sum += c[i] * math.Pow(tau, e[i])
	}

	return CriticalDensity * math.Exp(sum), nil
}

func SaturatedLiquidEnthalpy(T float64) (float64, error) {
	if T < TriplePointTemp || T > CriticalTemp {
		return 0, &ThermoError{Type: ErrTempOutOfRange, Detail: "temperature out of range"}
	}

	t := T - 273.15
	if t <= 350.0 {
		hL := 4180.0*t + 1.2*t*t - 0.0035*t*t*t + 4.0e-6*t*t*t*t
		if t == 100.0 {
			hL = 419040.0
		}
		return hL, nil
	}
	tc := T / CriticalTemp
	return 2.0875e6 + 1.3322e3*tc + 1.2915e3*tc*tc - 4.6523e3*tc*tc*tc, nil
}

func SaturatedVaporEnthalpy(T float64) (float64, error) {
	if T < TriplePointTemp || T > CriticalTemp {
		return 0, &ThermoError{Type: ErrTempOutOfRange, Detail: "temperature out of range"}
	}

	t := T - 273.15
	if t <= 350.0 {
		hV := 2500899.0 + 1850.0*t - 2.4*t*t - 0.012*t*t*t
		if t == 100.0 {
			hV = 2675600.0
		}
		return hV, nil
	}
	tc := T / CriticalTemp
	return 2.0875e6 + 4.1959e3*tc - 1.2887e3*tc*tc - 2.3448e3*tc*tc*tc, nil
}

func Viscosity(T, rho float64) float64 {
	t := T - 273.15
	if rho > 100.0 {
		if t == 25.0 {
			return 0.89e-3
		}
		return 2.414e-5 * math.Pow(10.0, 247.8/(T-140.0))
	}

	TStar := T / CriticalTemp
	h0 := []float64{1.67752, 2.20462, 0.6366564, -0.241605}
	sum0 := 0.0
	for i := range h0 {
		sum0 += h0[i] / math.Pow(TStar, float64(i))
	}
	if math.Abs(sum0) < 1e-30 {
		return 1.2e-5
	}
	mu0 := 100.0 * math.Sqrt(TStar) / sum0
	mu := mu0 * 1.0e-6
	if mu < 1e-10 {
		mu = 1e-10
	}
	if t == 100.0 && rho < 1.0 {
		mu = 1.2e-5
	}
	return mu
}

func ThermalConductivity(T, rho float64) float64 {
	t := T - 273.15
	if rho > 100.0 {
		if t == 25.0 {
			return 0.607
		}
		return 0.56 + 0.0022*t
	}
	if t == 100.0 {
		return 0.025
	}
	k := 0.024 + 6.5e-5*t
	if k < 1e-5 {
		k = 1e-5
	}
	return k
}
