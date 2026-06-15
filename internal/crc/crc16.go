package crc

const (
	crc16ModbusPoly uint16 = 0xA001
	crc16Init       uint16 = 0xFFFF
)

var crcTable [256]uint16

func init() {
	for i := 0; i < 256; i++ {
		crc := uint16(i)
		for j := 0; j < 8; j++ {
			if crc&0x0001 != 0 {
				crc = (crc >> 1) ^ crc16ModbusPoly
			} else {
				crc >>= 1
			}
		}
		crcTable[i] = crc
	}
}

func CRC16Modbus(data []byte) uint16 {
	crc := crc16Init
	for _, b := range data {
		crc = (crc >> 8) ^ crcTable[(crc^uint16(b))&0x00FF]
	}
	return crc
}

func ValidateCRC16(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	payloadLen := len(data) - 2
	calculated := CRC16Modbus(data[:payloadLen])
	received := uint16(data[payloadLen]) | (uint16(data[payloadLen+1]) << 8)
	return calculated == received
}

func AppendCRC16(data []byte) []byte {
	crc := CRC16Modbus(data)
	return append(data, byte(crc&0xFF), byte(crc>>8))
}
