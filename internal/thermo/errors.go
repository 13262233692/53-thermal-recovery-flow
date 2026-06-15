package thermo

type ThermoErrorType int

const (
	ErrTempOutOfRange ThermoErrorType = iota
	ErrPressureOutOfRange
	ErrDensityOutOfRange
	ErrConvergenceFailed
	ErrInvalidInput
	ErrSuperheated
	ErrSubcooled
)

type ThermoError struct {
	Type   ThermoErrorType
	Detail string
}

func (e *ThermoError) Error() string {
	switch e.Type {
	case ErrTempOutOfRange:
		return "temperature out of range: " + e.Detail
	case ErrPressureOutOfRange:
		return "pressure out of range: " + e.Detail
	case ErrDensityOutOfRange:
		return "density out of range: " + e.Detail
	case ErrConvergenceFailed:
		return "convergence failed: " + e.Detail
	case ErrInvalidInput:
		return "invalid input: " + e.Detail
	case ErrSuperheated:
		return "fluid is superheated: " + e.Detail
	case ErrSubcooled:
		return "fluid is subcooled: " + e.Detail
	default:
		return "thermodynamic error: " + e.Detail
	}
}
