package protocol

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"thermal-recovery-flow/internal/crc"
)

func TestDecodeModbusRTU(t *testing.T) {
	decoder := NewBitDecoder()

	payload := make([]byte, 18)
	binary.BigEndian.PutUint32(payload[0:4], 1500000)
	binary.BigEndian.PutUint32(payload[4:8], 30000)
	binary.BigEndian.PutUint16(payload[8:10], 0x1234)
	binary.BigEndian.PutUint32(payload[10:14], 567890)
	payload[14] = 0x01
	payload[15] = 0x00

	frameData := []byte{0x01, 0x03, byte(len(payload))}
	frameData = append(frameData, payload...)
	frameData = crc.AppendCRC16(frameData)

	frame, err := decoder.DecodeModbusRTU(frameData)
	if err != nil {
		t.Fatalf("DecodeModbusRTU() error = %v", err)
	}

	if !frame.IsValid {
		t.Error("Frame should be valid")
	}
	if frame.SlaveAddress != 0x01 {
		t.Errorf("SlaveAddress = %d, expected 1", frame.SlaveAddress)
	}
	if frame.FunctionCode != 0x03 {
		t.Errorf("FunctionCode = %d, expected 3", frame.FunctionCode)
	}
	if len(frame.Data) != len(payload) {
		t.Errorf("Data length = %d, expected %d", len(frame.Data), len(payload))
	}
}

func TestDecodeModbusRTUInvalidCRC(t *testing.T) {
	decoder := NewBitDecoder()

	frameData := []byte{0x01, 0x03, 0x02, 0x00, 0xFF, 0x00, 0x00}
	frame, err := decoder.DecodeModbusRTU(frameData)

	if err == nil {
		t.Error("Expected error for invalid CRC")
	}
	if frame == nil {
		t.Error("Frame should not be nil even with invalid CRC")
	} else if frame.IsValid {
		t.Error("Frame should not be valid")
	}
}

func TestDecodeModbusRTUFrameTooShort(t *testing.T) {
	decoder := NewBitDecoder()

	_, err := decoder.DecodeModbusRTU([]byte{0x01, 0x03})
	if err == nil {
		t.Error("Expected error for too short frame")
	}
}

func TestExtractSensorData(t *testing.T) {
	decoder := NewBitDecoder()

	payload := make([]byte, 16)
	binary.BigEndian.PutUint32(payload[0:4], 1500000)
	binary.BigEndian.PutUint32(payload[4:8], 30000)
	binary.BigEndian.PutUint16(payload[8:10], 0x0001)
	binary.BigEndian.PutUint32(payload[10:14], 0x00089A7A)
	payload[14] = 0x01
	payload[15] = 0x00

	frameData := []byte{0x01, 0x03, byte(len(payload))}
	frameData = append(frameData, payload...)
	frameData = crc.AppendCRC16(frameData)

	frame, err := decoder.DecodeModbusRTU(frameData)
	if err != nil {
		t.Fatalf("DecodeModbusRTU() error = %v", err)
	}

	sensorData, err := decoder.ExtractSensorData(frame)
	if err != nil {
		t.Fatalf("ExtractSensorData() error = %v", err)
	}

	expectedPressure := 1500.0
	if !approxEqual(sensorData.DifferentialPressure, expectedPressure, 0.01) {
		t.Errorf("DifferentialPressure = %f, expected %f", sensorData.DifferentialPressure, expectedPressure)
	}

	expectedTemp := 26.85
	if !approxEqual(sensorData.DryBulbTemp, expectedTemp, 0.01) {
		t.Errorf("DryBulbTemp = %f, expected %f", sensorData.DryBulbTemp, expectedTemp)
	}

	if sensorData.SensorType != SensorVortexFlowmeter {
		t.Errorf("SensorType = %d, expected %d", sensorData.SensorType, SensorVortexFlowmeter)
	}

	if sensorData.SlaveAddress != 0x01 {
		t.Errorf("SlaveAddress = %d, expected 1", sensorData.SlaveAddress)
	}
}

func TestExtractSensorDataInvalidFrame(t *testing.T) {
	decoder := NewBitDecoder()

	frame := &ModbusRTUFrame{
		SlaveAddress: 0x01,
		FunctionCode: 0x03,
		IsValid:      false,
	}

	_, err := decoder.ExtractSensorData(frame)
	if err == nil {
		t.Error("Expected error for invalid frame")
	}
}

func TestExtractSensorDataInsufficientData(t *testing.T) {
	decoder := NewBitDecoder()

	frame := &ModbusRTUFrame{
		SlaveAddress: 0x01,
		FunctionCode: 0x03,
		IsValid:      true,
		Data:         []byte{0x00, 0x01, 0x02},
	}

	_, err := decoder.ExtractSensorData(frame)
	if err == nil {
		t.Error("Expected error for insufficient data")
	}
}

func TestStreamDecode(t *testing.T) {
	decoder := NewBitDecoder()

	input := make(chan []byte, 10)
	output := make(chan *RawSensorData, 10)
	errors := make(chan error, 10)

	go decoder.StreamDecode(input, output, errors)

	payload := make([]byte, 16)
	binary.BigEndian.PutUint32(payload[0:4], 1500000)
	binary.BigEndian.PutUint32(payload[4:8], 30000)
	binary.BigEndian.PutUint16(payload[8:10], 0x0001)
	binary.BigEndian.PutUint32(payload[10:14], 0x00089A7A)
	payload[14] = 0x02
	payload[15] = 0x00

	frameData := []byte{0x02, 0x03, byte(len(payload))}
	frameData = append(frameData, payload...)
	frameData = crc.AppendCRC16(frameData)

	input <- frameData
	time.Sleep(100 * time.Millisecond)

	select {
	case sensorData := <-output:
		if sensorData.SensorType != SensorCapacitanceProdMeter {
			t.Errorf("SensorType = %d, expected %d", sensorData.SensorType, SensorCapacitanceProdMeter)
		}
		if sensorData.SlaveAddress != 0x02 {
			t.Errorf("SlaveAddress = %d, expected 2", sensorData.SlaveAddress)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for sensor data")
	}

	close(input)
}

func TestGetStats(t *testing.T) {
	decoder := NewBitDecoder()

	payload := make([]byte, 16)
	for i := range payload {
		payload[i] = byte(i)
	}

	goodFrame := []byte{0x01, 0x03, byte(len(payload))}
	goodFrame = append(goodFrame, payload...)
	goodFrame = crc.AppendCRC16(goodFrame)

	badFrame := []byte{0x01, 0x03, byte(len(payload))}
	badFrame = append(badFrame, payload...)
	badFrame = append(badFrame, 0x00, 0x00)

	for i := 0; i < 10; i++ {
		decoder.DecodeModbusRTU(goodFrame)
	}
	for i := 0; i < 5; i++ {
		decoder.DecodeModbusRTU(badFrame)
	}

	total, invalid, noise := decoder.GetStats()
	if total != 15 {
		t.Errorf("Total frames = %d, expected 15", total)
	}
	if invalid != 5 {
		t.Errorf("Invalid frames = %d, expected 5", invalid)
	}
	if noise != 5 {
		t.Errorf("Noise filtered = %d, expected 5", noise)
	}
}

func TestBytesToUint48(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected uint64
	}{
		{
			name:     "Zero",
			input:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expected: 0,
		},
		{
			name:     "Max 48-bit",
			input:    []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			expected: 0xFFFFFFFFFFFF,
		},
		{
			name:     "Known value",
			input:    []byte{0x00, 0x00, 0x01, 0x23, 0x45, 0x67},
			expected: 0x01234567,
		},
		{
			name:     "Short input",
			input:    []byte{0x01, 0x02},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := bytesToUint48(tt.input)
			if result != tt.expected {
				t.Errorf("bytesToUint48() = %d, expected %d", result, tt.expected)
			}
		})
	}
}

func TestFloat32Conversion(t *testing.T) {
	tests := []float32{0.0, 1.0, -1.0, 3.14159, 1000.5, -99.99}

	for _, expected := range tests {
		t.Run("Float32_Conversion", func(t *testing.T) {
			bytes := Float32ToBytes(expected)
			result := BytesToFloat32(bytes)
			if result != expected {
				t.Errorf("Float32 conversion = %f, expected %f", result, expected)
			}
		})
	}
}

func approxEqual(a, b, tolerance float64) bool {
	return math.Abs(a-b) < tolerance
}

func BenchmarkDecodeModbusRTU(b *testing.B) {
	decoder := NewBitDecoder()

	payload := make([]byte, 16)
	for i := range payload {
		payload[i] = byte(i)
	}

	frameData := []byte{0x01, 0x03, byte(len(payload))}
	frameData = append(frameData, payload...)
	frameData = crc.AppendCRC16(frameData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder.DecodeModbusRTU(frameData)
	}
}

func BenchmarkExtractSensorData(b *testing.B) {
	decoder := NewBitDecoder()

	payload := make([]byte, 16)
	for i := range payload {
		payload[i] = byte(i)
	}

	frameData := []byte{0x01, 0x03, byte(len(payload))}
	frameData = append(frameData, payload...)
	frameData = crc.AppendCRC16(frameData)

	frame, _ := decoder.DecodeModbusRTU(frameData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder.ExtractSensorData(frame)
	}
}
