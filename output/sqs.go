package output

import (
	"encoding/json"
	"sync"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	log "github.com/sirupsen/logrus"
)

type SqsConfig struct {
	QueueUrl string `json:"queueUrl"`
	Region   string `json:"region"`
}

type SQSOutput struct {
	QueueUrl string
	Region   string
	sqsSvc   sqsiface.SQSAPI
	wg       *sync.WaitGroup
}

func (o *SQSOutput) Init(...interface{}) error {
	session, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            aws.Config{Region: &o.Region},
	})
	if err != nil {
		return err
	}
	o.sqsSvc = sqs.New(session)
	o.wg = &sync.WaitGroup{}
	return nil
}

func (o *SQSOutput) Sink(input *chan interface{}) {
	log.Debugf("Writing to SQS queue %v", o.QueueUrl)
	o.wg.Add(1)
	defer o.wg.Done()

	for i := range *input {
		if i == nil {
			continue
		}

		data := i.(*OutputEvent)
		rawData, _ := json.Marshal(data)
		sendMessageParams := &sqs.SendMessageInput{
			MessageBody: aws.String(string(rawData)),
			QueueUrl:    &o.QueueUrl,
		}
		_, err := o.sqsSvc.SendMessage(sendMessageParams)
		if err != nil {
			log.Errorf("Unable to write to SQS Queue: %v\n", err)
		}
	}
}

func (o *SQSOutput) Close() error {
	o.wg.Wait()
	return nil
}
