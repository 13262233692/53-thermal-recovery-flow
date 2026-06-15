package driftflux

type SolverErrorType int

const (
	ErrInvalidTemp SolverErrorType = iota
	ErrInvalidPressure
	ErrInvalidMassFlux
	ErrConvergence
	ErrPropertyCalc
	ErrQualityOutOfRange
	ErrVoidFractionOutOfRange
)

type SolverError struct {
	Type   SolverErrorType
	Detail string
}

func (e *SolverError) Error() string {
	switch e.Type {
	case ErrInvalidTemp:
		return "invalid temperature: " + e.Detail
	case ErrInvalidPressure:
		return "invalid pressure: " + e.Detail
	case ErrInvalidMassFlux:
		return "invalid mass flux: " + e.Detail
	case ErrConvergence:
		return "convergence error: " + e.Detail
	case ErrPropertyCalc:
		return "property calculation error: " + e.Detail
	case ErrQualityOutOfRange:
		return "quality out of range: " + e.Detail
	case ErrVoidFractionOutOfRange:
		return "void fraction out of range: " + e.Detail
	default:
		return "solver error: " + e.Detail
	}
}
