package output

import (
	"encoding/json"
	"io"
	"os"
	"sync"

	log "github.com/Sirupsen/logrus"
)

type FileOutput struct {
	FileName string
	file     io.WriteCloser
}

func (f *FileOutput) Init() error {
	var err error
	f.file, err = os.OpenFile(f.FileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	return err
}

func (f *FileOutput) Sink(input *chan interface{}, wg *sync.WaitGroup) {
	log.Debugf("Writing to file %v", f.FileName)
	defer (*wg).Done()

	defer f.file.Close()

	for i := range *input {
		data := i.(*OutputEvent)
		rawData, _ := json.Marshal(data)
		_, err := f.file.Write(rawData)
		if err != nil {
			log.Fatalf("Unable to write to file %v: %v\n", f.FileName, err)
		}
	}
}
