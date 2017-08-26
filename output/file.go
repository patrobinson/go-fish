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
		if i == nil {
			continue
		}
		data, err := json.Marshal(i)
		if err != nil {
			log.Fatalf("Unable to write event to file: %v\n%v\n", err, data)
		}
		_, err = file.Write(data)
		if err != nil {
			log.Fatalf("Unable to write to file %v: %v\n", f.FileName, err)
		}
		err = file.Sync()
		if err != nil {
			log.Fatalf("Unable to sync file %v: %v\n", f.FileName, err)
		}
	}
}
