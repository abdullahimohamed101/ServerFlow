package config

// SchedulerConfig configures the request scheduling strategy. Strategy
// names the scheduling algorithm to use.
type SchedulerConfig struct {
	Strategy string `yaml:"strategy"`
}
