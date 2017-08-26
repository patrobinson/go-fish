package output

import (
	"encoding/json"
	"sync"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

type SQSOutput struct {
	QueueUrl string
	Region   string
	sqsSvc   sqsiface.SQSAPI
}

func (o *SQSOutput) Init() error {
	session, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            aws.Config{Region: &o.Region},
	})
	if err != nil {
		return err
	}
	o.sqsSvc = sqs.New(session)
	return nil
}

func (o *SQSOutput) Sink(input *chan interface{}, wg *sync.WaitGroup) {
	log.Debugf("Writing to SQS queue %v", o.QueueUrl)
	defer (*wg).Done()

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
