package safety

import (
	"fmt"
	"runtime/debug"
	"sync/atomic"

	"thermal-recovery-flow/pkg/logger"
)

var (
	totalPanics uint64
)

type PanicHandler func(r interface{}, stack []byte)

func SafeGo(log *logger.Logger, name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				atomic.AddUint64(&totalPanics, 1)
				if log != nil {
					log.Error("PANIC recovered in [%s]: %v\nStack trace:\n%s",
						name, r, string(stack))
				} else {
					fmt.Printf("PANIC recovered in [%s]: %v\nStack trace:\n%s\n",
						name, r, string(stack))
				}
			}
		}()
		fn()
	}()
}

func SafeGoWG(log *logger.Logger, name string, wgDone func(), fn func()) {
	go func() {
		defer func() {
			if wgDone != nil {
				wgDone()
			}
			if r := recover(); r != nil {
				stack := debug.Stack()
				atomic.AddUint64(&totalPanics, 1)
				if log != nil {
					log.Error("PANIC recovered in [%s]: %v\nStack trace:\n%s",
						name, r, string(stack))
				} else {
					fmt.Printf("PANIC recovered in [%s]: %v\nStack trace:\n%s\n",
						name, r, string(stack))
				}
			}
		}()
		fn()
	}()
}

func SafeRecover(log *logger.Logger, name string) {
	if r := recover(); r != nil {
		stack := debug.Stack()
		atomic.AddUint64(&totalPanics, 1)
		if log != nil {
			log.Error("PANIC recovered in [%s]: %v\nStack trace:\n%s",
				name, r, string(stack))
		} else {
			fmt.Printf("PANIC recovered in [%s]: %v\nStack trace:\n%s\n",
				name, r, string(stack))
		}
	}
}

func TotalPanics() uint64 {
	return atomic.LoadUint64(&totalPanics)
}
