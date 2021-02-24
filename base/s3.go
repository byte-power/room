package base

import (
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Service struct {
	s3Srv  *s3.S3
	bucket string
}

func NewS3Service(config S3Config, bucket string) (*S3Service, error) {
	sessionConfig := config.Session
	session, err := NewAWSSession(sessionConfig)
	if err != nil {
		return nil, err
	}
	service := &S3Service{bucket: bucket, s3Srv: s3.New(session)}
	return service, nil
}

func (service *S3Service) GetContentByKey(key string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(service.bucket),
		Key:    aws.String(key),
	}
	result, err := service.s3Srv.GetObject(input)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}
