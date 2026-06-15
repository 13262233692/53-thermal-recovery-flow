package driftflux

import (
	"math"
	"testing"
	"time"

	"thermal-recovery-flow/internal/protocol"
	"thermal-recovery-flow/pkg/logger"
)

func TestNewDriftFluxSolver(t *testing.T) {
	log, _ := logger.NewLogger(logger.LevelInfo, "")
	defer log.Close()

	decodedChan := make(chan *protocol.RawSensorData, 100)
	frameChan := make(chan *HydrodynamicsFrame, 100)
	errChan := make(chan error, 100)

	solver := NewDriftFluxSolver(DefaultConfig(), decodedChan, frameChan, errChan, log)

	if solver == nil {
		t.Fatal("NewDriftFluxSolver() returned nil")
	}

	stats := solver.GetStats()
	if stats.TotalFrames != 0 {
		t.Errorf("Initial TotalFrames = %d, expected 0", stats.TotalFrames)
	}
	if stats.FailedFrames != 0 {
		t.Errorf("Initial FailedFrames = %d, expected 0", stats.FailedFrames)
	}
}

func TestSolveBasic(t *testing.T) {
	log, _ := logger.NewLogger(logger.LevelInfo, "")
	defer log.Close()

	decodedChan := make(chan *protocol.RawSensorData, 100)
	frameChan := make(chan *HydrodynamicsFrame, 100)
	errChan := make(chan error, 100)

	solver := NewDriftFluxSolver(DefaultConfig(), decodedChan, frameChan, errChan, log)

	sensorData := &protocol.RawSensorData{
		Timestamp:            time.Now(),
		SensorType:           protocol.SensorVortexFlowmeter,
		SlaveAddress:         1,
		DifferentialPressure: 100000.0,
		DryBulbTemp:          200.0,
		HighSpeedTime:        123456789,
	}

	frame, err := solver.Solve(sensorData)
	if err != nil {
		t.Fatalf("Solve() error = %v", err)
	}

	if frame == nil {
		t.Fatal("Solve() returned nil frame")
	}

	if !frame.Converged {
		t.Error("Solve() did not converge")
	}

	if frame.Iterations <= 0 || frame.Iterations > 100 {
		t.Errorf("Iterations = %d, expected between 1 and 100", frame.Iterations)
	}

	if frame.SteamQuality < 0.0 || frame.SteamQuality > 1.0 {
		t.Errorf("SteamQuality = %f, expected between 0 and 1", frame.SteamQuality)
	}

	if frame.VoidFraction < 0.0 || frame.VoidFraction > 1.0 {
		t.Errorf("VoidFraction = %f, expected between 0 and 1", frame.VoidFraction)
	}

	if frame.MixtureDensity <= 0 {
		t.Errorf("MixtureDensity = %f, expected positive", frame.MixtureDensity)
	}

	if frame.Temperature != 200.0+273.15 {
		t.Errorf("Temperature = %f, expected %f", frame.Temperature, 200.0+273.15)
	}

	if frame.Pressure != 100000.0+101325.0 {
		t.Errorf("Pressure = %f, expected %f", frame.Pressure, 100000.0+101325.0)
	}

	if frame.SlaveAddress != 1 {
		t.Errorf("SlaveAddress = %d, expected 1", frame.SlaveAddress)
	}

	if frame.SensorType != protocol.SensorVortexFlowmeter {
		t.Errorf("SensorType = %d, expected %d", frame.SensorType, protocol.SensorVortexFlowmeter)
	}

	if frame.HighSpeedTime != 123456789 {
		t.Errorf("HighSpeedTime = %d, expected 123456789", frame.HighSpeedTime)
	}
}

func TestSolveDrySaturatedSteam(t *testing.T) {
	log, _ := logger.NewLogger(logger.LevelInfo, "")
	defer log.Close()

	decodedChan := make(chan *protocol.RawSensorData, 100)
	frameChan := make(chan *HydrodynamicsFrame, 100)
	errChan := make(chan error, 100)

	solver := NewDriftFluxSolver(DefaultConfig(), decodedChan, frameChan, errChan, log)

	sensorData := &protocol.RawSensorData{
		Timestamp:            time.Now(),
		SensorType:           protocol.SensorVortexFlowmeter,
		SlaveAddress:         2,
		DifferentialPressure: 50000.0,
		DryBulbTemp:          250.0,
		HighSpeedTime:        987654321,
	}

	frame, err := solver.Solve(sensorData)
	if err != nil {
		t.Fatalf("Solve() error = %v", err)
	}

	if frame.SteamQuality < 0.7 {
		t.Errorf("At high temperature, expected high steam quality, got %f", frame.SteamQuality)
	}

	if frame.VaporDensity >= frame.LiquidDensity {
		t.Error("Vapor density should be less than liquid density")
	}

	if frame.VaporVelocity <= frame.LiquidVelocity {
		t.Error("Vapor velocity should be greater than liquid velocity (slip)")
	}
}

func TestSolveEdgeCases(t *testing.T) {
	log, _ := logger.NewLogger(logger.LevelInfo, "")
	defer log.Close()

	decodedChan := make(chan *protocol.RawSensorData, 100)
	frameChan := make(chan *HydrodynamicsFrame, 100)
	errChan := make(chan error, 100)

	solver := NewDriftFluxSolver(DefaultConfig(), decodedChan, frameChan, errChan, log)

	tests := []struct {
		name        string
		sensorData  *protocol.RawSensorData
		expectError bool
	}{
		{
			name: "Low temperature",
			sensorData: &protocol.RawSensorData{
				DifferentialPressure: 100000.0,
				DryBulbTemp:          -10.0,
			},
			expectError: true,
		},
		{
			name: "High temperature",
			sensorData: &protocol.RawSensorData{
				DifferentialPressure: 100000.0,
				DryBulbTemp:          800.0,
			},
			expectError: true,
		},
		{
			name: "Normal temperature 100C",
			sensorData: &protocol.RawSensorData{
				DifferentialPressure: 100000.0,
				DryBulbTemp:          100.0,
			},
			expectError: false,
		},
		{
			name: "Normal temperature 300C",
			sensorData: &protocol.RawSensorData{
				DifferentialPressure: 500000.0,
				DryBulbTemp:          300.0,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := solver.Solve(tt.sensorData)
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestSolveDriftFluxIterations(t *testing.T) {
	log, _ := logger.NewLogger(logger.LevelInfo, "")
	defer log.Close()

	decodedChan := make(chan *protocol.RawSensorData, 100)
	frameChan := make(chan *HydrodynamicsFrame, 100)
	errChan := make(chan error, 100)

	config := DefaultConfig()
	config.MaxIterations = 100
	config.Tolerance = 1e-12
	solver := NewDriftFluxSolver(config, decodedChan, frameChan, errChan, log)

	sensorData := &protocol.RawSensorData{
		Timestamp:            time.Now(),
		DifferentialPressure: 200000.0,
		DryBulbTemp:          180.0,
	}

	frame, err := solver.Solve(sensorData)
	if err != nil {
		t.Fatalf("Solve() error = %v", err)
	}

	if !frame.Converged {
		t.Error("Expected convergence")
	}

	if frame.Iterations <= 0 {
		t.Errorf("Expected iterations > 0, got %d", frame.Iterations)
	}

	t.Logf("Converged in %d iterations, quality = %.6f, void fraction = %.6f",
		frame.Iterations, frame.SteamQuality, frame.VoidFraction)
}

func TestCalculateVelocities(t *testing.T) {
	log, _ := logger.NewLogger(logger.LevelInfo, "")
	defer log.Close()

	decodedChan := make(chan *protocol.RawSensorData, 100)
	frameChan := make(chan *HydrodynamicsFrame, 100)
	errChan := make(chan error, 100)

	solver := NewDriftFluxSolver(DefaultConfig(), decodedChan, frameChan, errChan, log)

	tests := []struct {
		name      string
		x         float64
		alpha     float64
		G         float64
		rhoL      float64
		rhoV      float64
		checkFunc func(t *testing.T, vL, vV, vm float64)
	}{
		{
			name:  "All liquid (x=0, alpha=0)",
			x:     0.0,
			alpha: 0.0,
			G:     1000.0,
			rhoL:  1000.0,
			rhoV:  1.0,
			checkFunc: func(t *testing.T, vL, vV, vm float64) {
				expectedVm := 1.0
				if math.Abs(vm-expectedVm) > 1e-6 {
					t.Errorf("vm = %f, expected %f", vm, expectedVm)
				}
				if vL != vm || vV != vm {
					t.Errorf("vL = %f, vV = %f, both should equal vm = %f", vL, vV, vm)
				}
			},
		},
		{
			name:  "All vapor (x=1, alpha=1)",
			x:     1.0,
			alpha: 1.0,
			G:     100.0,
			rhoL:  1000.0,
			rhoV:  1.0,
			checkFunc: func(t *testing.T, vL, vV, vm float64) {
				expectedVm := 100.0
				if math.Abs(vm-expectedVm) > 1e-6 {
					t.Errorf("vm = %f, expected %f", vm, expectedVm)
				}
				if vL != vm || vV != vm {
					t.Errorf("vL = %f, vV = %f, both should equal vm = %f", vL, vV, vm)
				}
			},
		},
		{
			name:  "Two-phase flow",
			x:     0.5,
			alpha: 0.9,
			G:     500.0,
			rhoL:  1000.0,
			rhoV:  1.0,
			checkFunc: func(t *testing.T, vL, vV, vm float64) {
				if vV <= vL {
					t.Errorf("vV = %f should be > vL = %f", vV, vL)
				}
				if vm <= 0 {
					t.Errorf("vm should be positive, got %f", vm)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vL, vV, vm := solver.calculateVelocities(tt.x, tt.alpha, tt.G, tt.rhoL, tt.rhoV)
			tt.checkFunc(t, vL, vV, vm)
		})
	}
}

func TestCalculateSlipVelocity(t *testing.T) {
	log, _ := logger.NewLogger(logger.LevelInfo, "")
	defer log.Close()

	decodedChan := make(chan *protocol.RawSensorData, 100)
	frameChan := make(chan *HydrodynamicsFrame, 100)
	errChan := make(chan error, 100)

	solver := NewDriftFluxSolver(DefaultConfig(), decodedChan, frameChan, errChan, log)

	slip := solver.calculateSlipVelocity(0.5, 0.8, 1000.0, 1.0)
	if slip <= 0 {
		t.Errorf("Slip velocity should be positive, got %f", slip)
	}

	slip = solver.calculateSlipVelocity(0.0, 0.0, 1000.0, 1.0)
	if slip != 0 {
		t.Errorf("Slip velocity should be 0 for all liquid, got %f", slip)
	}

	slip = solver.calculateSlipVelocity(1.0, 1.0, 1000.0, 1.0)
	if slip != 0 {
		t.Errorf("Slip velocity should be 0 for all vapor, got %f", slip)
	}
}

func TestProcessLoop(t *testing.T) {
	log, _ := logger.NewLogger(logger.LevelInfo, "")
	defer log.Close()

	decodedChan := make(chan *protocol.RawSensorData, 100)
	frameChan := make(chan *HydrodynamicsFrame, 100)
	errChan := make(chan error, 100)

	solver := NewDriftFluxSolver(DefaultConfig(), decodedChan, frameChan, errChan, log)
	solver.Start()

	sensorData := &protocol.RawSensorData{
		Timestamp:            time.Now(),
		SensorType:           protocol.SensorVortexFlowmeter,
		SlaveAddress:         1,
		DifferentialPressure: 150000.0,
		DryBulbTemp:          220.0,
		HighSpeedTime:        111222333,
	}

	decodedChan <- sensorData

	select {
	case frame := <-frameChan:
		if frame == nil {
			t.Fatal("Received nil frame")
		}
		if frame.HighSpeedTime != 111222333 {
			t.Errorf("HighSpeedTime = %d, expected 111222333", frame.HighSpeedTime)
		}
		t.Logf("Processed frame: quality=%.4f, void=%.4f, iterations=%d",
			frame.SteamQuality, frame.VoidFraction, frame.Iterations)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for frame")
	}

	solver.Stop()

	stats := solver.GetStats()
	if stats.TotalFrames != 1 {
		t.Errorf("TotalFrames = %d, expected 1", stats.TotalFrames)
	}
}

func BenchmarkSolve(b *testing.B) {
	log, _ := logger.NewLogger(logger.LevelInfo, "")
	defer log.Close()

	decodedChan := make(chan *protocol.RawSensorData, 100)
	frameChan := make(chan *HydrodynamicsFrame, 100)
	errChan := make(chan error, 100)

	solver := NewDriftFluxSolver(DefaultConfig(), decodedChan, frameChan, errChan, log)

	sensorData := &protocol.RawSensorData{
		Timestamp:            time.Now(),
		SensorType:           protocol.SensorVortexFlowmeter,
		SlaveAddress:         1,
		DifferentialPressure: 150000.0,
		DryBulbTemp:          220.0,
		HighSpeedTime:        111222333,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		solver.Solve(sensorData)
	}
}

func BenchmarkSolveDriftFlux(b *testing.B) {
	log, _ := logger.NewLogger(logger.LevelInfo, "")
	defer log.Close()

	decodedChan := make(chan *protocol.RawSensorData, 100)
	frameChan := make(chan *HydrodynamicsFrame, 100)
	errChan := make(chan error, 100)

	solver := NewDriftFluxSolver(DefaultConfig(), decodedChan, frameChan, errChan, log)

	eq := &DriftFluxEquation{
		Pressure:    200000.0,
		Temperature: 473.15,
		MassFlux:    1000.0,
		RhoL:        865.0,
		RhoV:        5.0,
		Hfg:         2000000.0,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		solver.solveDriftFlux(eq)
	}
}
