package orderbook

import (
	"math/rand"
	"sync"
)

var (
	conf    *cfg
	cfgOnce sync.Once
)

func init() {
	cfgOnce.Do(func() {
		conf = &cfg{
			maxLevel:    16,
			probability: 0.5,
			rng:         rand.New(rand.NewSource(42)),
		}
	})
}

type cfg struct {
	maxLevel    int
	probability float64
	rng         *rand.Rand
}
