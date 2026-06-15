package protocol

import (
	"encoding/binary"
	"math"
	"sync"
	"time"

	"thermal-recovery-flow/internal/crc"
)

type BitDecoder struct {
	mu              sync.RWMutex
	noiseFiltered   uint64
	totalFrames     uint64
	invalidFrames   uint64
}

func NewBitDecoder() *BitDecoder {
	return &BitDecoder{}
}

func (d *BitDecoder) DecodeModbusRTU(data []byte) (*ModbusRTUFrame, error) {
	if len(data) < MinFrameLength {
		return nil, &DecodeError{Type: ErrFrameTooShort, Detail: "insufficient bytes"}
	}

	frame := &ModbusRTUFrame{
		SlaveAddress: data[0],
		FunctionCode: data[1],
		RawLength:    len(data),
	}

	if len(data) > 5 {
		byteCount := int(data[2])
		if len(data) >= byteCount+5 {
			frame.Data = make([]byte, byteCount)
			copy(frame.Data, data[3:3+byteCount])
		}
	}

	valid := crc.ValidateCRC16(data)
	frame.IsValid = valid
	frame.CRC = binary.LittleEndian.Uint16(data[len(data)-2:])

	d.mu.Lock()
	d.totalFrames++
	if !valid {
		d.invalidFrames++
		d.noiseFiltered++
	}
	d.mu.Unlock()

	if !valid {
		return frame, &DecodeError{Type: ErrCRCInvalid, Detail: "electromagnetic noise detected"}
	}

	return frame, nil
}

func (d *BitDecoder) ExtractSensorData(frame *ModbusRTUFrame) (*RawSensorData, error) {
	if !frame.IsValid {
		return nil, &DecodeError{Type: ErrInvalidFrame, Detail: "cannot extract from invalid frame"}
	}

	data := frame.Data
	if len(data) < 16 {
		return nil, &DecodeError{Type: ErrInsufficientData, Detail: "need at least 16 bytes of register data"}
	}

	sensorData := &RawSensorData{
		SlaveAddress: frame.SlaveAddress,
		Timestamp:    time.Now(),
	}

	pressureRaw := bytesToUint32(data[0:4])
	sensorData.DifferentialPressure = convertToPressure(pressureRaw)

	tempRaw := bytesToUint32(data[4:8])
	sensorData.DryBulbTemp = convertToTemperature(tempRaw)

	sensorData.HighSpeedTime = bytesToUint48(data[8:14])

	if len(data) >= 16 {
		sensorType := data[14]
		switch sensorType {
		case 0x01:
			sensorData.SensorType = SensorVortexFlowmeter
		case 0x02:
			sensorData.SensorType = SensorCapacitanceProdMeter
		default:
			sensorData.SensorType = SensorVortexFlowmeter
		}
	}

	sensorData.RawBytes = make([]byte, len(data))
	copy(sensorData.RawBytes, data)

	return sensorData, nil
}

func (d *BitDecoder) StreamDecode(input <-chan []byte, output chan<- *RawSensorData, errors chan<- error) {
	for raw := range input {
		frame, err := d.DecodeModbusRTU(raw)
		if err != nil {
			select {
			case errors <- err:
			default:
			}
			continue
		}

		if !frame.IsValid {
			continue
		}

		sensorData, err := d.ExtractSensorData(frame)
		if err != nil {
			select {
			case errors <- err:
			default:
			}
			continue
		}

		select {
		case output <- sensorData:
		default:
		}
	}
}

func (d *BitDecoder) GetStats() (total, invalid, noiseFiltered uint64) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.totalFrames, d.invalidFrames, d.noiseFiltered
}

func bytesToUint16(b []byte) uint16 {
	return binary.BigEndian.Uint16(b)
}

func bytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func bytesToUint48(b []byte) uint64 {
	if len(b) < 6 {
		return 0
	}
	return uint64(b[0])<<40 | uint64(b[1])<<32 | uint64(b[2])<<24 |
		uint64(b[3])<<16 | uint64(b[4])<<8 | uint64(b[5])
}

func convertToPressure(raw uint32) float64 {
	const pressureScale = 0.001
	const pressureOffset = 0.0
	return (float64(raw) * pressureScale) + pressureOffset
}

func convertToTemperature(raw uint32) float64 {
	const tempScale = 0.01
	const tempOffset = -273.15
	return (float64(raw) * tempScale) + tempOffset
}

func Float32ToBytes(f float32) []byte {
	bits := math.Float32bits(f)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, bits)
	return b
}

func BytesToFloat32(b []byte) float32 {
	bits := binary.BigEndian.Uint32(b)
	return math.Float32frombits(bits)
}
