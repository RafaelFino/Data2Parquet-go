package writer

import (
	"bytes"
	"context"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/xitongsys/parquet-go/parquet"
)

type S3 struct {
	config          *config.Config
	client          *s3.Client
	ctx             context.Context
	compressionType parquet.CompressionCodec
}

func NewS3(ctx context.Context, config *config.Config) Writer {
	if ctx == nil {
		ctx = context.Background()
	}

	ret := &S3{
		config:          config,
		ctx:             ctx,
		compressionType: GetCompressionType(config.WriterCompressionType),
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

func (s *S3) Write(key string, data []*domain.Record) []*WriterReturn {
	start := time.Now()

	buf := &bytes.Buffer{}

	ret := WriteParquet(key, data, buf, s.config.WriterRowGroupSize, s.compressionType)

	if CheckWriterError(ret) {
		for _, r := range ret {
			if r.Error != nil {
				slog.Error("Error writing parquet file", "error", r.Error, "module", "writer", "function", "Write", "key", key)
			}
		}

		return ret
	}

	slog.Debug("Data written on buffer", "key", key, "module", "writer.file", "function", "Write", "records", len(data), "duration", time.Since(start))

	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.config.S3BuketName),
		Key:    aws.String(s.makeBuketName(key)),
		Body:   buf,
	})

	if err != nil {
		slog.Error("Error writing to S3", "error", err, "module", "writer.file", "function", "Write", "key", key)
		ret = append(ret, &WriterReturn{Error: err})
	}

	return ret
}

func (s *S3) makeBuketName(key string) string {
	tm := time.Now()
	year, month, day := tm.Date()
	hour, min, sec := tm.Clock()

	return fmt.Sprintf("%s/%d/%02d/%02d/%02d%02d%02d %s.parquet", s.config.S3BuketName, year, month, day, hour, min, sec, key)
}

func (s *S3) Close() error {
	slog.Debug("Closing AWS-S3 writer")

	<-s.ctx.Done()
	return nil
}

func (s *S3) IsReady() bool {
	return s.client != nil
}
