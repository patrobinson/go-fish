package output

import (
	"encoding/json"
	"os"
	"sync"

	log "github.com/Sirupsen/logrus"
)

type FileOutput struct {
	FileName string
}

func (f *FileOutput) Sink(input *chan interface{}, wg *sync.WaitGroup) {
	log.Debugf("Writing to file %v", f.FileName)
	defer (*wg).Done()
	file, err := os.OpenFile(f.FileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Fatal("Unable to open file %v: %v", f.FileName, err)
	}
	defer file.Close()

	for i := range *input {
		data := i.(*OutputEvent)
		rawData, _ := json.Marshal(data)
		_, err := file.Write(rawData)
		if err != nil {
			log.Fatalf("Unable to write to file %v: %v\n", f.FileName, err)
		}
	}
}
