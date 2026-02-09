package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "circuit"

var (
	once sync.Once
)

func init() {
	once.Do(func() {
		prometheus.MustRegister(
			requestCountByState,
			stateTransitionHistogram,
		)
	})
}
