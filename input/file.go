package input

import (
	"bufio"
	"os"

	log "github.com/sirupsen/logrus"
)

type FileConfig struct {
	Path string `json:"path"`
}

type FileInput struct {
	FileName string
}

func (i *FileInput) Init() error {
	return nil
}

func (i *FileInput) Retrieve(output *chan []byte) {
	defer close(*output)
	file, err := os.Open(i.FileName)
	if err != nil {
		log.Fatalf("Unable to open file %v: %v\n", i.FileName, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		*output <- scanner.Bytes()
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Unable to read file %v: %v\n", i.FileName, err)
	}
}
