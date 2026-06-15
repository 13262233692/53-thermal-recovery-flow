package thermo

import (
	"math"
	"testing"
)

func TestSaturationPressure(t *testing.T) {
	tests := []struct {
		name        string
		temp        float64
		expectedMin float64
		expectedMax float64
	}{
		{
			name:        "At 100°C (373.15K)",
			temp:        373.15,
			expectedMin: 95000.0,
			expectedMax: 105000.0,
		},
		{
			name:        "At 150°C (423.15K)",
			temp:        423.15,
			expectedMin: 450000.0,
			expectedMax: 500000.0,
		},
		{
			name:        "At 200°C (473.15K)",
			temp:        473.15,
			expectedMin: 1500000.0,
			expectedMax: 1600000.0,
		},
		{
			name:        "At 300°C (573.15K)",
			temp:        573.15,
			expectedMin: 8500000.0,
			expectedMax: 9000000.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SaturationPressure(tt.temp)
			if err != nil {
				t.Fatalf("SaturationPressure() error = %v", err)
			}

			if result < tt.expectedMin || result > tt.expectedMax {
				t.Errorf("SaturationPressure() = %f Pa, expected between %f and %f Pa",
					result, tt.expectedMin, tt.expectedMax)
			}
		})
	}
}

func TestSaturationPressureOutOfRange(t *testing.T) {
	_, err := SaturationPressure(200.0)
	if err == nil {
		t.Error("Expected error for temperature below triple point")
	}

	_, err = SaturationPressure(700.0)
	if err == nil {
		t.Error("Expected error for temperature above critical point")
	}
}

func TestSaturationTemperature(t *testing.T) {
	tests := []struct {
		name        string
		pressure    float64
		expectedMin float64
		expectedMax float64
	}{
		{
			name:        "At 1 atm (101325 Pa)",
			pressure:    101325.0,
			expectedMin: 373.0,
			expectedMax: 373.5,
		},
		{
			name:        "At 1 MPa (1000000 Pa)",
			pressure:    1000000.0,
			expectedMin: 452.0,
			expectedMax: 454.0,
		},
		{
			name:        "At 5 MPa (5000000 Pa)",
			pressure:    5000000.0,
			expectedMin: 536.0,
			expectedMax: 538.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SaturationTemperature(tt.pressure)
			if err != nil {
				t.Fatalf("SaturationTemperature() error = %v", err)
			}

			if result < tt.expectedMin || result > tt.expectedMax {
				t.Errorf("SaturationTemperature() = %f K, expected between %f and %f K",
					result, tt.expectedMin, tt.expectedMax)
			}
		})
	}
}

func TestSaturationTemperatureOutOfRange(t *testing.T) {
	_, err := SaturationTemperature(100.0)
	if err == nil {
		t.Error("Expected error for pressure below triple point")
	}

	_, err = SaturationTemperature(30e6)
	if err == nil {
		t.Error("Expected error for pressure above critical point")
	}
}

func TestSaturatedLiquidDensity(t *testing.T) {
	tests := []struct {
		name        string
		temp        float64
		expectedMin float64
		expectedMax float64
	}{
		{
			name:        "At 100°C (373.15K)",
			temp:        373.15,
			expectedMin: 958.0,
			expectedMax: 959.0,
		},
		{
			name:        "At 200°C (473.15K)",
			temp:        473.15,
			expectedMin: 864.0,
			expectedMax: 866.0,
		},
		{
			name:        "At 300°C (573.15K)",
			temp:        573.15,
			expectedMin: 712.0,
			expectedMax: 714.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SaturatedLiquidDensity(tt.temp)
			if err != nil {
				t.Fatalf("SaturatedLiquidDensity() error = %v", err)
			}

			if result < tt.expectedMin || result > tt.expectedMax {
				t.Errorf("SaturatedLiquidDensity() = %f kg/m³, expected between %f and %f kg/m³",
					result, tt.expectedMin, tt.expectedMax)
			}
		})
	}
}

func TestSaturatedVaporDensity(t *testing.T) {
	rhoL, err := SaturatedLiquidDensity(373.15)
	if err != nil {
		t.Fatalf("SaturatedLiquidDensity() error = %v", err)
	}

	rhoV, err := SaturatedVaporDensity(373.15)
	if err != nil {
		t.Fatalf("SaturatedVaporDensity() error = %v", err)
	}

	if rhoV >= rhoL {
		t.Error("Vapor density should be less than liquid density")
	}

	if rhoV < 0.5 || rhoV > 0.6 {
		t.Errorf("SaturatedVaporDensity() at 100°C = %f kg/m³, expected ~0.598 kg/m³", rhoV)
	}
}

func TestSaturatedEnthalpies(t *testing.T) {
	hL, err := SaturatedLiquidEnthalpy(373.15)
	if err != nil {
		t.Fatalf("SaturatedLiquidEnthalpy() error = %v", err)
	}

	hV, err := SaturatedVaporEnthalpy(373.15)
	if err != nil {
		t.Fatalf("SaturatedVaporEnthalpy() error = %v", err)
	}

	if hL < 419000 || hL > 420000 {
		t.Errorf("SaturatedLiquidEnthalpy() at 100°C = %f J/kg, expected ~419040 J/kg", hL)
	}

	if hV < 2675000 || hV > 2676000 {
		t.Errorf("SaturatedVaporEnthalpy() at 100°C = %f J/kg, expected ~2675600 J/kg", hV)
	}

	hfg := hV - hL
	if hfg < 2255000 || hfg > 2257000 {
		t.Errorf("Latent heat at 100°C = %f J/kg, expected ~2256500 J/kg", hfg)
	}
}

func TestViscosity(t *testing.T) {
	muWater := Viscosity(298.15, 997.0)
	if muWater < 0.8e-3 || muWater > 1.0e-3 {
		t.Errorf("Viscosity of water at 25°C = %f Pa·s, expected ~0.89e-3 Pa·s", muWater)
	}

	muSteam := Viscosity(373.15, 0.598)
	if muSteam < 10e-6 || muSteam > 20e-6 {
		t.Errorf("Viscosity of steam at 100°C = %f Pa·s, expected ~12e-6 Pa·s", muSteam)
	}
}

func TestThermalConductivity(t *testing.T) {
	kWater := ThermalConductivity(298.15, 997.0)
	if kWater < 0.6 || kWater > 0.62 {
		t.Errorf("Thermal conductivity of water at 25°C = %f W/m·K, expected ~0.607 W/m·K", kWater)
	}

	kSteam := ThermalConductivity(373.15, 0.598)
	if kSteam < 0.02 || kSteam > 0.03 {
		t.Errorf("Thermal conductivity of steam at 100°C = %f W/m·K, expected ~0.025 W/m·K", kSteam)
	}
}

func TestCriticalPoint(t *testing.T) {
	ps, _ := SaturationPressure(CriticalTemp - 0.01)
	if math.Abs(ps-CriticalPress) > 1e6 {
		t.Errorf("Saturation pressure near critical point = %f Pa, expected ~%f Pa", ps, CriticalPress)
	}

	rhoL, _ := SaturatedLiquidDensity(CriticalTemp - 0.01)
	rhoV, _ := SaturatedVaporDensity(CriticalTemp - 0.01)

	if math.Abs(rhoL-CriticalDensity) > 50 {
		t.Errorf("Liquid density near critical point = %f kg/m³, expected ~%f kg/m³", rhoL, CriticalDensity)
	}

	if math.Abs(rhoV-CriticalDensity) > 50 {
		t.Errorf("Vapor density near critical point = %f kg/m³, expected ~%f kg/m³", rhoV, CriticalDensity)
	}
}

func BenchmarkSaturationPressure(b *testing.B) {
	T := 473.15
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SaturationPressure(T)
	}
}

func BenchmarkSaturationTemperature(b *testing.B) {
	P := 1e6
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SaturationTemperature(P)
	}
}

func BenchmarkSaturatedLiquidDensity(b *testing.B) {
	T := 473.15
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SaturatedLiquidDensity(T)
	}
}

func BenchmarkViscosity(b *testing.B) {
	T := 473.15
	rho := 865.0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Viscosity(T, rho)
	}
}
