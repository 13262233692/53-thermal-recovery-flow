package protocol

import (
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"thermal-recovery-flow/internal/crc"
	"thermal-recovery-flow/internal/safety"
)

const (
	MaxAllowedFrameLength = 512
	MinFrameLengthConst   = 5
)

type BitDecoder struct {
	mu             sync.RWMutex
	noiseFiltered  uint64
	totalFrames    uint64
	invalidFrames  uint64
	truncatedFrames uint64
	oversizedFrames uint64
}

func NewBitDecoder() *BitDecoder {
	return &BitDecoder{}
}

func (d *BitDecoder) DecodeModbusRTU(data []byte) (frame *ModbusRTUFrame, err error) {
	defer func() {
		if r := recover(); r != nil {
			safety.SafeRecover(nil, "protocol.DecodeModbusRTU")
			atomic.AddUint64(&d.invalidFrames, 1)
			frame = nil
			err = &DecodeError{Type: ErrInvalidFrame, Detail: "panic during decode"}
		}
	}()

	if len(data) == 0 {
		atomic.AddUint64(&d.invalidFrames, 1)
		return nil, &DecodeError{Type: ErrFrameTooShort, Detail: "empty frame"}
	}

	if len(data) > MaxAllowedFrameLength {
		atomic.AddUint64(&d.oversizedFrames, 1)
		atomic.AddUint64(&d.invalidFrames, 1)
		return nil, &DecodeError{Type: ErrFrameTooLong, Detail: "frame exceeds max allowed length"}
	}

	if len(data) < MinFrameLength {
		atomic.AddUint64(&d.truncatedFrames, 1)
		atomic.AddUint64(&d.invalidFrames, 1)
		return nil, &DecodeError{Type: ErrFrameTooShort, Detail: "insufficient bytes"}
	}

	atomic.AddUint64(&d.totalFrames, 1)

	frame = &ModbusRTUFrame{
		SlaveAddress: data[0],
		FunctionCode: data[1],
		RawLength:    len(data),
	}

	if len(data) > 2 {
		byteCount := int(data[2])
		if byteCount < 0 {
			atomic.AddUint64(&d.invalidFrames, 1)
			frame.IsValid = false
			return frame, &DecodeError{Type: ErrInvalidData, Detail: "negative byte count"}
		}
		if byteCount > 255 {
			atomic.AddUint64(&d.invalidFrames, 1)
			frame.IsValid = false
			return frame, &DecodeError{Type: ErrInvalidData, Detail: "byte count exceeds limit"}
		}
		if byteCount > 0 && len(data) >= byteCount+5 {
			frame.Data = make([]byte, byteCount)
			copy(frame.Data, data[3:3+byteCount])
		} else if byteCount > 0 {
			atomic.AddUint64(&d.truncatedFrames, 1)
		}
	}

	valid := crc.ValidateCRC16(data)
	frame.IsValid = valid

	if len(data) >= 2 {
		crcOffset := len(data) - 2
		if crcOffset >= 0 && crcOffset+2 <= len(data) {
			frame.CRC = binary.LittleEndian.Uint16(data[crcOffset:])
		}
	}

	if !valid {
		atomic.AddUint64(&d.invalidFrames, 1)
		atomic.AddUint64(&d.noiseFiltered, 1)
		return frame, &DecodeError{Type: ErrCRCInvalid, Detail: "electromagnetic noise detected, CRC mismatch"}
	}

	return frame, nil
}

func (d *BitDecoder) ExtractSensorData(frame *ModbusRTUFrame) (data *RawSensorData, err error) {
	defer func() {
		if r := recover(); r != nil {
			safety.SafeRecover(nil, "protocol.ExtractSensorData")
			data = nil
			err = &DecodeError{Type: ErrInvalidFrame, Detail: "panic during sensor extraction"}
		}
	}()

	if frame == nil {
		return nil, &DecodeError{Type: ErrInvalidFrame, Detail: "nil frame"}
	}

	if !frame.IsValid {
		return nil, &DecodeError{Type: ErrInvalidFrame, Detail: "cannot extract from invalid frame"}
	}

	if len(frame.Data) < 16 {
		return nil, &DecodeError{Type: ErrInsufficientData, Detail: "need at least 16 bytes of register data"}
	}

	data = &RawSensorData{
		SlaveAddress: frame.SlaveAddress,
		Timestamp:    time.Now(),
	}

	if len(frame.Data) >= 4 {
		pressureRaw := bytesToUint32(frame.Data[0:4])
		data.DifferentialPressure = convertToPressure(pressureRaw)
	}

	if len(frame.Data) >= 8 {
		tempRaw := bytesToUint32(frame.Data[4:8])
		data.DryBulbTemp = convertToTemperature(tempRaw)
	}

	if len(frame.Data) >= 14 {
		data.HighSpeedTime = bytesToUint48(frame.Data[8:14])
	}

	if len(frame.Data) >= 16 {
		sensorType := frame.Data[14]
		switch sensorType {
		case 0x01:
			data.SensorType = SensorVortexFlowmeter
		case 0x02:
			data.SensorType = SensorCapacitanceProdMeter
		default:
			data.SensorType = SensorVortexFlowmeter
		}
	}

	data.RawBytes = make([]byte, len(frame.Data))
	copy(data.RawBytes, frame.Data)

	return data, nil
}

func (d *BitDecoder) StreamDecode(input <-chan []byte, output chan<- *RawSensorData, errors chan<- error) {
	defer safety.SafeRecover(nil, "protocol.StreamDecode")

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
	return atomic.LoadUint64(&d.totalFrames),
		atomic.LoadUint64(&d.invalidFrames),
		atomic.LoadUint64(&d.noiseFiltered)
}

func (d *BitDecoder) GetExtendedStats() (total, invalid, noise, truncated, oversized uint64) {
	return atomic.LoadUint64(&d.totalFrames),
		atomic.LoadUint64(&d.invalidFrames),
		atomic.LoadUint64(&d.noiseFiltered),
		atomic.LoadUint64(&d.truncatedFrames),
		atomic.LoadUint64(&d.oversizedFrames)
}

func bytesToUint16(b []byte) uint16 {
	if len(b) < 2 {
		return 0
	}
	return binary.BigEndian.Uint16(b)
}

func bytesToUint32(b []byte) uint32 {
	if len(b) < 4 {
		return 0
	}
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
	p := (float64(raw) * pressureScale) + pressureOffset
	if math.IsNaN(p) || math.IsInf(p, 0) {
		return 0.0
	}
	return p
}

func convertToTemperature(raw uint32) float64 {
	const tempScale = 0.01
	const tempOffset = -273.15
	t := (float64(raw) * tempScale) + tempOffset
	if math.IsNaN(t) || math.IsInf(t, 0) {
		return 0.0
	}
	return t
}

func Float32ToBytes(f float32) []byte {
	bits := math.Float32bits(f)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, bits)
	return b
}

func BytesToFloat32(b []byte) float32 {
	if len(b) < 4 {
		return 0
	}
	bits := binary.BigEndian.Uint32(b)
	return math.Float32frombits(bits)
}
