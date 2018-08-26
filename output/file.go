package output

import (
	"encoding/json"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

type FileConfig struct {
	Path string `json:"path"`
}

type FileOutput struct {
	FileName string
	file     *os.File
	wg       *sync.WaitGroup
}

func (f *FileOutput) Init() error {
	var err error
	if _, err := os.Stat(f.FileName); os.IsNotExist(err) {
		os.Create(f.FileName)
	}
	f.wg = &sync.WaitGroup{}
	f.file, err = os.OpenFile(f.FileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	return err
}

func (f *FileOutput) Sink(input *chan interface{}) {
	log.Debugf("Writing to file %v", f.FileName)
	f.wg.Add(1)
	defer f.wg.Done()
	defer f.file.Close()

	for i := range *input {
		if i == nil {
			continue
		}
		data, err := json.Marshal(i)
		if err != nil {
			log.Fatalf("Unable to write event to file: %v\n%v\n", err, data)
		}
		_, err = f.file.Write(append(data, []byte("\n")...))
		if err != nil {
			log.Fatalf("Unable to write to file %v: %v\n", f.FileName, err)
		}
		err = f.file.Sync()
		if err != nil {
			log.Fatalf("Unable to sync file %v: %v\n", f.FileName, err)
		}
	}
}

func (f *FileOutput) Close() error {
	f.wg.Wait()
	return nil
}
