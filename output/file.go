package output

import (
	"os"
	log "github.com/Sirupsen/logrus"
)

type FileOutput struct {
	FileName string
}

func (f FileOutput) Sink(input chan interface{}) {
	file, err := os.Open(f.FileName)
	if err != nil {
		log.Fatal("Unable to open file %v: %v", f.FileName, err)
	}
	defer file.Close()

	for i := range input {
		in := i.(bool)
		var data []byte
		if in {
			data = []byte("true\n")
		} else {
			data = []byte("false\n")
		}
		_, err := file.Write(data)
		if err != nil {
			log.Fatalf("Unable to write to file %v: %v\n", f.FileName, err)
		}
	}
}
