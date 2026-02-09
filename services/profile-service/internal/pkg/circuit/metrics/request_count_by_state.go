package metrics

import "github.com/prometheus/client_golang/prometheus"

type RequestCountByStateError string

const (
	NoError         RequestCountByStateError = "nil"
	OpenState       RequestCountByStateError = "open_state"
	TooManyRequests RequestCountByStateError = "too_many_requests"
	Other           RequestCountByStateError = "other"
)

var requestCountByState = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "circuit_requests_attempted_total",
	Help:      "total number of CB interceptor calls with results",
}, []string{"name", "error"}) // error can be one of ["nil", "open_state", "too_many_requests, "other"]

func IncRequestCountByState(name string, error RequestCountByStateError) {
	requestCountByState.WithLabelValues(name, string(error)).Inc()
}
