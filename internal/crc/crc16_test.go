package crc

import (
	"testing"
)

func TestCRC16Modbus(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected uint16
	}{
		{
			name:     "Standard Modbus test frame",
			data:     []byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x0A},
			expected: 0xCDC5,
		},
		{
			name:     "Single byte",
			data:     []byte{0x01},
			expected: 0x807E,
		},
		{
			name:     "Empty data",
			data:     []byte{},
			expected: 0xFFFF,
		},
		{
			name:     "Known good frame",
			data:     []byte{0x11, 0x03, 0x00, 0x6B, 0x00, 0x03},
			expected: 0x8776,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CRC16Modbus(tt.data)
			if result != tt.expected {
				t.Errorf("CRC16Modbus() = 0x%04X, expected 0x%04X", result, tt.expected)
			}
		})
	}
}

func TestValidateCRC16(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{
			name:     "Valid Modbus RTU frame",
			data:     append([]byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x0A}, 0xC5, 0xCD),
			expected: true,
		},
		{
			name:     "Invalid CRC",
			data:     append([]byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x0A}, 0x00, 0x00),
			expected: false,
		},
		{
			name:     "Frame too short",
			data:     []byte{0x01, 0x03},
			expected: false,
		},
		{
			name:     "Another valid frame",
			data:     append([]byte{0x11, 0x03, 0x00, 0x6B, 0x00, 0x03}, 0x76, 0x87),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValidateCRC16(tt.data)
			if result != tt.expected {
				t.Errorf("ValidateCRC16() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestAppendCRC16(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Simple frame",
			data: []byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x0A},
		},
		{
			name: "Another frame",
			data: []byte{0x11, 0x03, 0x00, 0x6B, 0x00, 0x03},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AppendCRC16(tt.data)
			if len(result) != len(tt.data)+2 {
				t.Errorf("AppendCRC16() returned length %d, expected %d", len(result), len(tt.data)+2)
			}
			if !ValidateCRC16(result) {
				t.Error("AppendCRC16() produced invalid CRC")
			}
		})
	}
}

func TestCRC16NoiseFilter(t *testing.T) {
	payload := []byte{0x01, 0x03, 0x02, 0x00, 0xFF}
	goodFrame := AppendCRC16(payload)
	if !ValidateCRC16(goodFrame) {
		t.Error("Valid frame failed CRC check")
	}

	noisyFrame := make([]byte, len(goodFrame))
	copy(noisyFrame, goodFrame)
	noisyFrame[3] ^= 0xAA
	if ValidateCRC16(noisyFrame) {
		t.Error("Noisy frame passed CRC check - filter not working")
	}

	corruptedFrame := make([]byte, len(goodFrame))
	copy(corruptedFrame, goodFrame)
	corruptedFrame[2] = 0xFF
	if ValidateCRC16(corruptedFrame) {
		t.Error("Corrupted frame passed CRC check")
	}
}

func BenchmarkCRC16Modbus(b *testing.B) {
	data := []byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CRC16Modbus(data)
	}
}

func BenchmarkValidateCRC16(b *testing.B) {
	data := append([]byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x0A}, 0xCD, 0xC5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ValidateCRC16(data)
	}
}
