package writer

import (
	"bytes"
	"context"
	"data2parquet/pkg/config"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3 struct {
	config *config.Config
	client *s3.Client
	ctx    context.Context
}

func NewS3(ctx context.Context, config *config.Config) Writer {
	if ctx == nil {
		ctx = context.Background()
	}

	ret := &S3{
		config: config,
		ctx:    ctx,
	}

	return ret
}

func (s *S3) Init() error {
	slog.Debug("Initializing S3 writer", "config", s.config.ToString())

	// Load the Shared AWS Configuration (~/.aws/config)]
	cfg, err := awsConfig.LoadDefaultConfig(s.ctx, awsConfig.WithRegion(s.config.S3Region))
	if err != nil {
		slog.Error("Error loading AWS config", "error", err, "module", "writer.file", "function", "Init")
		return err
	}

	// Create an Amazon S3 service client
	s.client = s3.NewFromConfig(cfg)

	if s.client == nil {
		slog.Error("Error creating S3 client", "module", "writer.file", "function", "Init")
		return errors.New("error creating S3 client")
	}

	return nil
}

func (s *S3) Write(key string, buf *bytes.Buffer) error {
	start := time.Now()

	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.config.S3BuketName),
		Key:    aws.String(s.makeBuketName(key)),
		Body:   buf,
	})

	if err != nil {
		slog.Error("Error writing to S3", "error", err, "module", "writer.file", "function", "Write", "key", key)
	}

	slog.Info("S3 written", "module", "writer.file", "function", "Write", "key", key, "duration", time.Since(start), "file-size", buf.Len())

	return err
}

func (s *S3) makeBuketName(key string) string {
	tm := time.Now()
	year, month, day := tm.Date()
	hour, min, sec := tm.Clock()

	return fmt.Sprintf("%s/%d/%02d/%02d/%02d%02d%02d %s.parquet", s.config.S3BuketName, year, month, day, hour, min, sec, key)
}

func (s *S3) Close() error {
	slog.Debug("Closing AWS-S3 writer")
	return nil
}

func (s *S3) IsReady() bool {
	return s.client != nil
}
