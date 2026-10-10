package config

// AdmissionConfig configures cluster admission control thresholds.
type AdmissionConfig struct {
	MaxGlobalRequests int `yaml:"max_global_requests"`
}
