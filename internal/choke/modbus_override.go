package choke

import (
	"encoding/binary"
	"fmt"
	"time"

	"thermal-recovery-flow/internal/crc"
)

const (
	FuncWriteSingleRegister  uint8 = 0x06
	FuncWriteMultipleRegisters uint8 = 0x10
	OverridePriorityEmergency byte  = 0xFF
	OverridePriorityHigh      byte  = 0x80
	OverridePriorityNormal    byte  = 0x40
)

type OverrideCommand struct {
	SlaveAddress   uint8
	FunctionCode   uint8
	RegisterAddr   uint16
	RegisterValue  uint16
	Priority       byte
	Timestamp      time.Time
	SequenceNumber uint32
	Reason         string
	Assessment     SafetyAssessment
}

type OverridePacket struct {
	RawBytes  []byte
	Command   OverrideCommand
	AssembleTime time.Duration
}

var overrideSeqNum uint32

func BuildEmergencyOverride(slaveAddr uint8, registerAddr uint16, opening float64, assessment SafetyAssessment) OverridePacket {
	start := time.Now()
	atomicAddSeq()

	openingPermille := uint16(opening * 1000.0)
	if openingPermille > 1000 {
		openingPermille = 1000
	}

	priority := OverridePriorityHigh
	if assessment.LockdownRequired {
		priority = OverridePriorityEmergency
		openingPermille = 0
	}

	cmd := OverrideCommand{
		SlaveAddress:   slaveAddr,
		FunctionCode:   FuncWriteSingleRegister,
		RegisterAddr:   registerAddr,
		RegisterValue:  openingPermille,
		Priority:       priority,
		Timestamp:      time.Now(),
		SequenceNumber: overrideSeqNum,
		Reason:         threatLevelString(assessment.ThreatLevel),
		Assessment:     assessment,
	}

	raw := assembleModbusWriteSingle(cmd)

	elapsed := time.Since(start)

	return OverridePacket{
		RawBytes:     raw,
		Command:      cmd,
		AssembleTime: elapsed,
	}
}

func BuildLockdownOverride(slaveAddr uint8, registerAddr uint16, assessment SafetyAssessment) OverridePacket {
	start := time.Now()
	atomicAddSeq()

	cmd := OverrideCommand{
		SlaveAddress:   slaveAddr,
		FunctionCode:   FuncWriteSingleRegister,
		RegisterAddr:   registerAddr,
		RegisterValue:  0,
		Priority:       OverridePriorityEmergency,
		Timestamp:      time.Now(),
		SequenceNumber: overrideSeqNum,
		Reason:         "LOCKDOWN:" + threatLevelString(assessment.ThreatLevel),
		Assessment:     assessment,
	}

	raw := assembleModbusWriteSingle(cmd)

	elapsed := time.Since(start)

	return OverridePacket{
		RawBytes:     raw,
		Command:      cmd,
		AssembleTime: elapsed,
	}
}

func BuildMultiRegisterOverride(slaveAddr uint8, startRegister uint16, values []uint16, assessment SafetyAssessment) OverridePacket {
	start := time.Now()
	atomicAddSeq()

	priority := OverridePriorityHigh
	if assessment.LockdownRequired {
		priority = OverridePriorityEmergency
	}

	cmd := OverrideCommand{
		SlaveAddress:   slaveAddr,
		FunctionCode:   FuncWriteMultipleRegisters,
		RegisterAddr:   startRegister,
		RegisterValue:  values[0],
		Priority:       priority,
		Timestamp:      time.Now(),
		SequenceNumber: overrideSeqNum,
		Reason:         threatLevelString(assessment.ThreatLevel),
		Assessment:     assessment,
	}

	raw := assembleModbusWriteMultiple(cmd, values)

	elapsed := time.Since(start)

	return OverridePacket{
		RawBytes:     raw,
		Command:      cmd,
		AssembleTime: elapsed,
	}
}

func assembleModbusWriteSingle(cmd OverrideCommand) []byte {
	buf := make([]byte, 6)

	buf[0] = cmd.SlaveAddress
	buf[1] = cmd.FunctionCode
	binary.BigEndian.PutUint16(buf[2:4], cmd.RegisterAddr)
	binary.BigEndian.PutUint16(buf[4:6], cmd.RegisterValue)

	return crc.AppendCRC16(buf)
}

func assembleModbusWriteMultiple(cmd OverrideCommand, values []uint16) []byte {
	n := len(values)
	buf := make([]byte, 7+n*2)

	buf[0] = cmd.SlaveAddress
	buf[1] = cmd.FunctionCode
	binary.BigEndian.PutUint16(buf[2:4], cmd.RegisterAddr)
	binary.BigEndian.PutUint16(buf[4:6], uint16(n))
	buf[6] = byte(n * 2)

	for i, v := range values {
		binary.BigEndian.PutUint16(buf[7+i*2:9+i*2], v)
	}

	return crc.AppendCRC16(buf)
}

func ParseOpeningFromRegisterValue(value uint16) float64 {
	return float64(value) / 1000.0
}

func OpeningToRegisterValue(opening float64) uint16 {
	v := uint16(opening * 1000.0)
	if v > 1000 {
		v = 1000
	}
	return v
}

func (p OverridePacket) HexString() string {
	hex := fmt.Sprintf("%02X", p.Command.Priority)
	for _, b := range p.RawBytes {
		hex += fmt.Sprintf("%02X", b)
	}
	return hex
}

func (p OverridePacket) Description() string {
	return fmt.Sprintf("Override[#%d] slave=%d reg=0x%04X val=%d(%.3f) priority=0x%02X reason=%s asmTime=%v",
		p.Command.SequenceNumber,
		p.Command.SlaveAddress,
		p.Command.RegisterAddr,
		p.Command.RegisterValue,
		ParseOpeningFromRegisterValue(p.Command.RegisterValue),
		p.Command.Priority,
		p.Command.Reason,
		p.AssembleTime,
	)
}

func threatLevelString(level ThreatLevel) string {
	switch level {
	case ThreatNormal:
		return "NORMAL"
	case ThreatElevated:
		return "ELEVATED"
	case ThreatCritical:
		return "CRITICAL"
	case ThreatImminent:
		return "IMMINENT"
	default:
		return "UNKNOWN"
	}
}

func atomicAddSeq() uint32 {
	overrideSeqNum++
	return overrideSeqNum
}
