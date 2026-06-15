package protocol

type DecodeErrorType int

const (
	ErrFrameTooShort DecodeErrorType = iota
	ErrCRCInvalid
	ErrInvalidFrame
	ErrInsufficientData
	ErrInvalidFunctionCode
	ErrInvalidSlaveAddress
	ErrFrameTooLong
	ErrInvalidData
)

type DecodeError struct {
	Type   DecodeErrorType
	Detail string
}

func (e *DecodeError) Error() string {
	switch e.Type {
	case ErrFrameTooShort:
		return "frame too short: " + e.Detail
	case ErrCRCInvalid:
		return "CRC validation failed: " + e.Detail
	case ErrInvalidFrame:
		return "invalid frame: " + e.Detail
	case ErrInsufficientData:
		return "insufficient data: " + e.Detail
	case ErrInvalidFunctionCode:
		return "invalid function code: " + e.Detail
	case ErrInvalidSlaveAddress:
		return "invalid slave address: " + e.Detail
	case ErrFrameTooLong:
		return "frame too long: " + e.Detail
	case ErrInvalidData:
		return "invalid data: " + e.Detail
	default:
		return "decode error: " + e.Detail
	}
}
