package choke

import (
	"math"
	"testing"
	"time"

	"thermal-recovery-flow/internal/crc"
)

func TestCriticalPressureRatio(t *testing.T) {
	rCritical := CriticalPressureRatio(GammaSteam)
	if rCritical <= 0 || rCritical >= 1 {
		t.Errorf("Critical pressure ratio = %f, expected between 0 and 1", rCritical)
	}
	expectedApprox := 0.546
	if math.Abs(rCritical-expectedApprox) > 0.01 {
		t.Errorf("Critical pressure ratio = %f, expected approximately %f", rCritical, expectedApprox)
	}
}

func TestSonicVelocity(t *testing.T) {
	a := SonicVelocity(473.15, GammaSteam)
	if a <= 0 {
		t.Errorf("Sonic velocity = %f, expected positive", a)
	}
	expectedApprox := 500.0
	if math.Abs(a-expectedApprox) > 100 {
		t.Errorf("Sonic velocity at 200°C = %f, expected approximately %f m/s", a, expectedApprox)
	}
}

func TestSolveChokeFlowSubsonic(t *testing.T) {
	config := DefaultChokeValveConfig()
	upstreamP := 5.0e6
	upstreamT := 473.15
	upstreamRho := 25.0
	downstreamP := 3.0e6
	valveOpening := 0.5

	result := SolveChokeFlow(config, upstreamP, upstreamT, upstreamRho, downstreamP, valveOpening)

	if result.FlowState != FlowSubsonic {
		t.Errorf("Expected subsonic flow, got %d", result.FlowState)
	}
	if result.IsChoked {
		t.Error("Flow should not be choked")
	}
	if result.MassFlow <= 0 {
		t.Errorf("Mass flow = %f, expected positive", result.MassFlow)
	}
	if result.Velocity <= 0 {
		t.Errorf("Velocity = %f, expected positive", result.Velocity)
	}
	if result.MachNumber <= 0 || result.MachNumber >= 1.0 {
		t.Errorf("Mach number = %f, expected between 0 and 1 for subsonic", result.MachNumber)
	}
	if result.EffectiveArea <= 0 {
		t.Errorf("Effective area = %f, expected positive", result.EffectiveArea)
	}
}

func TestSolveChokeFlowChoked(t *testing.T) {
	config := DefaultChokeValveConfig()
	upstreamP := 10.0e6
	upstreamT := 573.15
	upstreamRho := 50.0
	downstreamP := 0.5e6
	valveOpening := 1.0

	result := SolveChokeFlow(config, upstreamP, upstreamT, upstreamRho, downstreamP, valveOpening)

	if !result.IsChoked {
		t.Error("Flow should be choked with large pressure ratio difference")
	}
	if result.MachNumber != 1.0 {
		t.Errorf("Mach number for choked flow = %f, expected 1.0", result.MachNumber)
	}
	if result.MassFlow <= 0 {
		t.Errorf("Mass flow = %f, expected positive", result.MassFlow)
	}
}

func TestSolveChokeFlowZeroOpening(t *testing.T) {
	config := DefaultChokeValveConfig()
	result := SolveChokeFlow(config, 5e6, 473.15, 25.0, 1e6, 0.0)

	if result.ValveOpening < MinOpening {
		t.Errorf("Valve opening = %f, expected at least MinOpening", result.ValveOpening)
	}
}

func TestSolveChokeFlowInvalidInput(t *testing.T) {
	config := DefaultChokeValveConfig()

	result := SolveChokeFlow(config, 0, 473.15, 25.0, 1e6, 0.5)
	if result.FlowState != FlowUnknown {
		t.Errorf("Expected FlowUnknown for zero upstream pressure, got %d", result.FlowState)
	}

	result = SolveChokeFlow(config, 5e6, 0, 25.0, 1e6, 0.5)
	if result.FlowState != FlowUnknown {
		t.Errorf("Expected FlowUnknown for zero temperature, got %d", result.FlowState)
	}
}

func TestComputeRequiredOpening(t *testing.T) {
	config := DefaultChokeValveConfig()
	upstreamP := 5.0e6
	upstreamT := 473.15
	upstreamRho := 25.0
	downstreamP := 3.0e6
	targetFlow := 10.0

	opening := ComputeRequiredOpening(config, upstreamP, upstreamT, upstreamRho, downstreamP, targetFlow)

	if opening < 0 || opening > 1.0 {
		t.Errorf("Required opening = %f, expected between 0 and 1", opening)
	}
}

func TestComputeRequiredOpeningZeroFlow(t *testing.T) {
	config := DefaultChokeValveConfig()
	opening := ComputeRequiredOpening(config, 5e6, 473.15, 25.0, 3e6, 0)
	if opening != MaxOpening {
		t.Errorf("Expected MaxOpening for zero flow, got %f", opening)
	}
}

func TestSafetyControllerNormalPressure(t *testing.T) {
	config := DefaultSafetyControllerConfig()
	config.SafetyPressureLimit = 35.0e6
	controller := NewSafetyController(config)

	assessment := controller.Assess(10.0e6, 473.15, 5.0, 101325.0, 1.0)

	if assessment.ThreatLevel != ThreatNormal {
		t.Errorf("Expected ThreatNormal at 10 MPa, got %d", assessment.ThreatLevel)
	}
	if assessment.RequiresOverride {
		t.Error("Should not require override at normal pressure")
	}
	if assessment.LockdownRequired {
		t.Error("Should not require lockdown at normal pressure")
	}
}

func TestSafetyControllerCriticalPressure(t *testing.T) {
	config := DefaultSafetyControllerConfig()
	config.SafetyPressureLimit = 35.0e6
	controller := NewSafetyController(config)

	controller.Assess(10.0e6, 473.15, 5.0, 101325.0, 1.0)
	controller.Assess(20.0e6, 473.15, 5.0, 101325.0, 1.0)
	controller.Assess(34.0e6, 473.15, 50.0, 101325.0, 1.0)

	assessment := controller.Assess(36.0e6, 473.15, 100.0, 101325.0, 1.0)

	if !assessment.RequiresOverride {
		t.Error("Should require override above safety limit")
	}
	if assessment.ThreatLevel < ThreatCritical {
		t.Errorf("Expected at least ThreatCritical, got %d", assessment.ThreatLevel)
	}
}

func TestSafetyControllerLockdown(t *testing.T) {
	config := DefaultSafetyControllerConfig()
	config.SafetyPressureLimit = 35.0e6
	controller := NewSafetyController(config)

	controller.Assess(10.0e6, 473.15, 5.0, 101325.0, 1.0)
	controller.Assess(20.0e6, 473.15, 5.0, 101325.0, 1.0)
	controller.Assess(30.0e6, 473.15, 50.0, 101325.0, 1.0)
	controller.Assess(35.0e6, 473.15, 100.0, 101325.0, 1.0)

	assessment := controller.Assess(40.0e6, 473.15, 200.0, 101325.0, 1.0)

	if !assessment.LockdownRequired {
		t.Error("Should require lockdown above safety limit with surge")
	}
	if assessment.RequiredOpening != 0.0 {
		t.Errorf("Lockdown should set opening to 0, got %f", assessment.RequiredOpening)
	}
}

func TestSafetyControllerPressurePrediction(t *testing.T) {
	config := DefaultSafetyControllerConfig()
	config.SafetyPressureLimit = 35.0e6
	controller := NewSafetyController(config)

	controller.Assess(10.0e6, 473.15, 5.0, 101325.0, 1.0)
	controller.Assess(15.0e6, 473.15, 5.0, 101325.0, 1.0)
	controller.Assess(25.0e6, 473.15, 5.0, 101325.0, 1.0)

	assessment := controller.Assess(30.0e6, 473.15, 20.0, 101325.0, 1.0)

	t.Logf("At 30 MPa: threat=%d, dP/dt=%.2e, predicted=%.2f MPa, margin=%.3f, tBreach=%v",
		assessment.ThreatLevel, assessment.PressureDerivative,
		assessment.PredictedPressure, assessment.SafetyMargin, assessment.TimeToBreach)

	if assessment.SafetyMargin <= 0 {
		t.Errorf("Safety margin should be positive at 30 MPa, got %f", assessment.SafetyMargin)
	}
}

func TestSafetyControllerReset(t *testing.T) {
	config := DefaultSafetyControllerConfig()
	controller := NewSafetyController(config)

	controller.Assess(10.0e6, 473.15, 5.0, 101325.0, 1.0)
	controller.Assess(20.0e6, 473.15, 5.0, 101325.0, 1.0)

	total, _, _ := controller.GetStats()
	if total != 2 {
		t.Errorf("Expected 2 assessments, got %d", total)
	}

	controller.Reset()
	total, _, _ = controller.GetStats()
	if total != 0 {
		t.Errorf("Expected 0 assessments after reset, got %d", total)
	}
}

func TestBuildEmergencyOverride(t *testing.T) {
	config := DefaultSafetyControllerConfig()
	controller := NewSafetyController(config)

	controller.Assess(10.0e6, 473.15, 5.0, 101325.0, 1.0)
	controller.Assess(25.0e6, 473.15, 5.0, 101325.0, 1.0)
	assessment := controller.Assess(36.0e6, 473.15, 100.0, 101325.0, 1.0)

	packet := BuildEmergencyOverride(0x01, 0x0100, 0.5, assessment)

	if len(packet.RawBytes) < 8 {
		t.Errorf("Packet too short: %d bytes", len(packet.RawBytes))
	}
	if packet.Command.SlaveAddress != 0x01 {
		t.Errorf("Slave address = %d, expected 1", packet.Command.SlaveAddress)
	}
	if packet.Command.FunctionCode != FuncWriteSingleRegister {
		t.Errorf("Function code = %d, expected %d", packet.Command.FunctionCode, FuncWriteSingleRegister)
	}
	if packet.Command.RegisterAddr != 0x0100 {
		t.Errorf("Register address = 0x%04X, expected 0x0100", packet.Command.RegisterAddr)
	}
	if packet.Command.Priority < OverridePriorityHigh {
		t.Errorf("Priority = 0x%02X, expected at least 0x%02X", packet.Command.Priority, OverridePriorityHigh)
	}

	t.Logf("Override packet: %s", packet.HexString())
	t.Logf("Description: %s", packet.Description())
	t.Logf("Assembly time: %v", packet.AssembleTime)
}

func TestBuildLockdownOverride(t *testing.T) {
	config := DefaultSafetyControllerConfig()
	config.SafetyPressureLimit = 35.0e6
	controller := NewSafetyController(config)

	controller.Assess(10.0e6, 473.15, 5.0, 101325.0, 1.0)
	controller.Assess(20.0e6, 473.15, 5.0, 101325.0, 1.0)
	controller.Assess(30.0e6, 473.15, 50.0, 101325.0, 1.0)
	controller.Assess(35.0e6, 473.15, 100.0, 101325.0, 1.0)
	assessment := controller.Assess(40.0e6, 473.15, 200.0, 101325.0, 1.0)

	packet := BuildLockdownOverride(0x01, 0x0100, assessment)

	if packet.Command.RegisterValue != 0 {
		t.Errorf("Lockdown value = %d, expected 0", packet.Command.RegisterValue)
	}
	if packet.Command.Priority != OverridePriorityEmergency {
		t.Errorf("Lockdown priority = 0x%02X, expected 0x%02X",
			packet.Command.Priority, OverridePriorityEmergency)
	}
}

func TestBuildMultiRegisterOverride(t *testing.T) {
	config := DefaultSafetyControllerConfig()
	controller := NewSafetyController(config)
	assessment := controller.Assess(10.0e6, 473.15, 5.0, 101325.0, 1.0)

	values := []uint16{500, 300, 100}
	packet := BuildMultiRegisterOverride(0x01, 0x0100, values, assessment)

	if len(packet.RawBytes) < 14 {
		t.Errorf("Multi-register packet too short: %d bytes", len(packet.RawBytes))
	}
	if packet.Command.FunctionCode != FuncWriteMultipleRegisters {
		t.Errorf("Function code = %d, expected %d", packet.Command.FunctionCode, FuncWriteMultipleRegisters)
	}
}

func TestOverrideCRCValidation(t *testing.T) {
	config := DefaultSafetyControllerConfig()
	controller := NewSafetyController(config)
	assessment := controller.Assess(10.0e6, 473.15, 5.0, 101325.0, 1.0)

	packet := BuildEmergencyOverride(0x01, 0x0100, 0.5, assessment)

	if !crc.ValidateCRC16(packet.RawBytes) {
		t.Errorf("Override packet CRC validation failed. Packet: % X", packet.RawBytes)
	}
}

func TestParseOpeningFromRegisterValue(t *testing.T) {
	tests := []struct {
		value    uint16
		expected float64
	}{
		{0, 0.0},
		{500, 0.5},
		{1000, 1.0},
		{250, 0.25},
	}

	for _, tt := range tests {
		result := ParseOpeningFromRegisterValue(tt.value)
		if math.Abs(result-tt.expected) > 1e-6 {
			t.Errorf("ParseOpeningFromRegisterValue(%d) = %f, expected %f",
				tt.value, result, tt.expected)
		}
	}
}

func TestOpeningToRegisterValue(t *testing.T) {
	result := OpeningToRegisterValue(0.5)
	if result != 500 {
		t.Errorf("OpeningToRegisterValue(0.5) = %d, expected 500", result)
	}

	result = OpeningToRegisterValue(1.5)
	if result != 1000 {
		t.Errorf("OpeningToRegisterValue(1.5) = %d, expected 1000 (clamped)", result)
	}
}

func TestOverrideAssemblySpeed(t *testing.T) {
	config := DefaultSafetyControllerConfig()
	controller := NewSafetyController(config)
	assessment := controller.Assess(30.0e6, 473.15, 50.0, 101325.0, 1.0)

	start := time.Now()
	for i := 0; i < 10000; i++ {
		BuildEmergencyOverride(0x01, 0x0100, 0.5, assessment)
	}
	elapsed := time.Since(start)

	avgPerPacket := elapsed / 10000
	t.Logf("Average override packet assembly time: %v", avgPerPacket)

	if avgPerPacket > 10*time.Microsecond {
		t.Errorf("Packet assembly too slow: %v per packet (expected < 10μs)", avgPerPacket)
	}
}

func TestThreatLevelString(t *testing.T) {
	tests := []struct {
		level    ThreatLevel
		expected string
	}{
		{ThreatNormal, "NORMAL"},
		{ThreatElevated, "ELEVATED"},
		{ThreatCritical, "CRITICAL"},
		{ThreatImminent, "IMMINENT"},
	}

	for _, tt := range tests {
		result := threatLevelString(tt.level)
		if result != tt.expected {
			t.Errorf("threatLevelString(%d) = %s, expected %s", tt.level, result, tt.expected)
		}
	}
}
