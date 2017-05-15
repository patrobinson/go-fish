package input

import (
	"bufio"
	"os"
	log "github.com/Sirupsen/logrus"
)

type FileInput struct {
	FileName string
}

func (i FileInput) Retrieve(output chan interface{}) {
	defer close(output)
	file, err := os.Open(i.FileName)
	if err != nil {
		log.Fatalf("Unable to open file %v: %v\n", i.FileName, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
      output <- scanner.Text()
 	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Unable to read file %v: %v\n", i.FileName, err)
	}
}
