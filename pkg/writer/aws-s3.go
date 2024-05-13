package writer

import (
	"context"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"errors"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3 struct {
	config *config.Config
	client *s3.Client
}

func NewS3(config *config.Config) Writer {
	return &S3{
		config: config,
	}
}

func (s *S3) Init() error {
	slog.Debug("Initializing S3 writer", "config", s.config.ToString())

	// Load the Shared AWS Configuration (~/.aws/config)
	awsCfg, err := awsConfig.LoadDefaultConfig(context.TODO())

	if err != nil {
		slog.Error("Error loading AWS config", "error", err)
		return err
	}

	// Create an Amazon S3 service client
	slog.Debug("Creating S3 client")
	s.client = s3.NewFromConfig(awsCfg)

	if s.client == nil {
		slog.Error("Error creating S3 client")
		return errors.New("Error creating S3 client")
	}

	return nil
}

func (s *S3) Write(data []*domain.Record) error {
	slog.Debug("Writing logs", "data", data)

	file, err := os.Open("data2parquet.parquet")

	if err != nil {
		slog.Error("fail to try open file")
	}

	bucketName := ""
	objectKey := ""

	defer file.Close()
	_, err = s.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   file,
	})

	return err
}

func (s *S3) Close() error {
	slog.Debug("Closing AWS-S3 writer")
	return nil
}

func (s *S3) IsReady() bool {
	panic("NOT IMPLEMENTED YET!!!")
}
