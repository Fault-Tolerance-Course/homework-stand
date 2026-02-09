package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sony/gobreaker/v2"
)

var stateTransitionHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: namespace,
	Name:      "circuit_state_transition_duration_seconds",
	Help:      "duration in state 'from' before moving to state 'to' in seconds",
	Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
}, []string{"name", "from", "to"})

func ObserveStateTransitionDuration(name string, from, to gobreaker.State, duration float64) {
	stateTransitionHistogram.WithLabelValues(name, from.String(), to.String()).Observe(duration)
}
