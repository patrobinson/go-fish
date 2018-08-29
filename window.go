package main

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type windowManager struct {
	rule     Rule
	sinkChan *chan interface{}
	sync.RWMutex
	interval   int
	lastCalled time.Time
	closeChan  *chan struct{}
}

func (w *windowManager) start() {
	closeChan := make(chan struct{})
	w.closeChan = &closeChan
	go func(closeChan chan struct{}) {
		for {
			w.windowRunner()
			select {
			case <-closeChan:
				break
			case <-time.After(1 * time.Second):
			}
		}
	}(closeChan)
}

func (w *windowManager) stop() {
	close(*w.closeChan)
}

func (w *windowManager) windowRunner() {
	if time.Now().Sub(w.lastCalled).Seconds() > float64(w.interval) {
		outputs, err := w.rule.Window()
		if err != nil {
			log.Errorf("Error calling Window() on rule %v: %v", w.rule.String(), err)
		}
		for _, o := range outputs {
			*w.sinkChan <- o
		}
		w.lastCalled = time.Now()
	}
}
