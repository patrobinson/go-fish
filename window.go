package main

import (
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

type windowManager struct {
	rules    []windowConfig
	sinkChan *chan interface{}
	sync.RWMutex
}

type windowConfig struct {
	rule       Rule
	interval   int
	lastCalled time.Time
}

func (w *windowManager) add(config windowConfig) {
	w.Lock()
	defer w.Unlock()
	w.rules = append(w.rules, config)
}

func (w *windowManager) start() {
	go func() {
		for {
			for _, ruleConfig := range w.rules {
				w.windowRunner(&ruleConfig)
			}
			time.Sleep(1 * time.Second)
		}
	}()
}

func (w *windowManager) windowRunner(ruleConfig *windowConfig) {
	if time.Now().Sub(ruleConfig.lastCalled).Seconds() > float64(ruleConfig.interval) {
		outputs, err := ruleConfig.rule.Window()
		if err != nil {
			log.Errorf("Error calling Window() on rule %v: %v", ruleConfig.rule.String(), err)
		}
		for _, o := range outputs {
			*w.sinkChan <- o
		}
		ruleConfig.lastCalled = time.Now()
	}
}
