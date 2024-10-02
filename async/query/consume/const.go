package consume

import "time"

const (
	SLAMicro  = time.Millisecond * 10
	SLAShort  = time.Second * 5
	SLAMedium = time.Second * 60
	SLALarge  = time.Minute * 10
	SLALong   = time.Minute * 60

	BatchSizeXS BatchSize = 1024
	BatchSizeS            = 2048
	BatchSizeM            = 4096
	BatchSizeL            = 8192
	BatchSizeXL           = 16384
)

// SLA defines the maximum time-period where eventual consistency
// between event-store and projections / read-models can occur
// Ability to make adjustments should be done on startup and via ENV as config value
// Default SLA is set to SLAShort which is 5s
type SLA time.Duration
type BatchSize uint16
