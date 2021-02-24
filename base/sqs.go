package base

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SQSService struct {
	sqsSrv   *sqs.SQS
	config   SQSConfig
	queueURL *string
}

func NewSQSService(config SQSConfig) (*SQSService, error) {
	sessionConfig := config.Session
	session, err := NewAWSSession(sessionConfig)
	if err != nil {
		return nil, err
	}
	service := &SQSService{config: config, sqsSrv: sqs.New(session)}

	getQueueURLinput := &sqs.GetQueueUrlInput{QueueName: aws.String(service.config.Queue)}
	getQueueURLResult, err := service.sqsSrv.GetQueueUrl(getQueueURLinput)
	if err != nil {
		return nil, err
	}
	service.queueURL = getQueueURLResult.QueueUrl
	return service, nil
}

func (service *SQSService) ReceiveMessages() ([]*sqs.Message, error) {
	config := service.config
	input := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages:   aws.Int64(config.MaxReceivedMessages),
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		QueueUrl:              service.queueURL,
		VisibilityTimeout:     aws.Int64(config.VisibilityTimeoutSeconds),
		WaitTimeSeconds:       aws.Int64(config.WaitTimeSeconds),
	}
	receiveMsgResult, err := service.sqsSrv.ReceiveMessage(input)
	if err != nil {
		return nil, err
	}
	if receiveMsgResult == nil {
		return nil, nil
	}
	return receiveMsgResult.Messages, nil
}

func (service *SQSService) DeleteMessage(message *sqs.Message) error {
	input := &sqs.DeleteMessageInput{
		QueueUrl:      service.queueURL,
		ReceiptHandle: message.ReceiptHandle,
	}
	_, err := service.sqsSrv.DeleteMessage(input)
	return err
}

type S3NotificationEvent struct {
	Records []S3NotificationEventRecord `json:"records"`
}

type S3NotificationEventRecord struct {
	S3 S3InfoInEvent `json:"s3"`
}

type S3InfoInEvent struct {
	Bucket S3BucketInfoInEvent `json:"bucket"`
	Object S3ObjectInfoInEvent `json:"object"`
}

type S3BucketInfoInEvent struct {
	Name string `json:"name"`
}

type S3ObjectInfoInEvent struct {
	Key string `json:"key"`
}

type S3MetaInfo struct {
	Bucket string
	Key    string
}

func GetS3MetaInfoFromNotificationMessage(message *sqs.Message) ([]S3MetaInfo, error) {
	body := message.Body
	if body == nil {
		return nil, errors.New("message body is null")
	}
	event := &S3NotificationEvent{}
	if err := json.Unmarshal([]byte(*body), event); err != nil {
		return nil, err
	}
	S3MetaInfoSlice := make([]S3MetaInfo, len(event.Records))
	for index, record := range event.Records {
		S3MetaInfoSlice[index] = S3MetaInfo{
			Bucket: record.S3.Bucket.Name,
			Key:    record.S3.Object.Key}
	}
	return S3MetaInfoSlice, nil
}
