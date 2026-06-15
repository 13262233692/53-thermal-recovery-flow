package protocol

import "time"

type SensorType uint8

const (
	SensorVortexFlowmeter SensorType = iota + 1
	SensorCapacitanceProdMeter
)

type RawSensorData struct {
	Timestamp     time.Time
	SensorType    SensorType
	SlaveAddress  uint8
	DifferentialPressure float64
	DryBulbTemp   float64
	HighSpeedTime uint64
	RawBytes      []byte
}

type ModbusRTUFrame struct {
	SlaveAddress uint8
	FunctionCode uint8
	Data         []byte
	CRC          uint16
	IsValid      bool
	RawLength    int
}

const (
	FuncReadHoldingRegisters uint8 = 0x03
	FuncReadInputRegisters   uint8 = 0x04
	MinFrameLength                 = 5
	MaxFrameLength                 = 256
)

const (
	RegPressureHigh    uint16 = 0x0000
	RegPressureLow     uint16 = 0x0001
	RegTempHigh        uint16 = 0x0002
	RegTempLow         uint16 = 0x0003
	RegTimeStampHigh   uint16 = 0x0004
	RegTimeStampMid    uint16 = 0x0005
	RegTimeStampLow    uint16 = 0x0006
	RegSensorType      uint16 = 0x0007
)
