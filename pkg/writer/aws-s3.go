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
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
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

	endpoints := map[string]string{
		"S3":  s.config.S3Endpoint,
		"STS": s.config.S3STSEndpoint,
	}

	cfg, err := awsConfig.LoadDefaultConfig(s.ctx,
		awsConfig.WithRegion(s.config.S3Region),
		//awsConfig.WithClientLogMode(aws.LogRetries|aws.LogRequest|aws.LogResponse|aws.LogResponseWithBody),
		awsConfig.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
			if endpoint, ok := endpoints[service]; ok {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           endpoint,
					SigningRegion: s.config.S3Region,
				}, nil
			}
			// returning EndpointNotFoundError will allow the service to fallback to its default resolution
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})),
	)

	if err != nil {
		slog.Error("Error loading AWS config", "error", err, "module", "writer.s3", "function", "Init")
		return err
	}

	roleARN := fmt.Sprintf("arn:aws:iam::%s:role/%s", s.config.S3Account, s.config.S3RoleName)

	stsClient := sts.NewFromConfig(cfg)

	cfg.Credentials = aws.NewCredentialsCache(
		stscreds.NewAssumeRoleProvider(stsClient, roleARN, func(aro *stscreds.AssumeRoleOptions) {
			aro.RoleSessionName = s.config.S3RoleName
		}),
	)

	// Create an Amazon S3 service client
	s.client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	if s.client == nil {
		slog.Error("Error creating S3 client", "module", "writer.s3", "function", "Init")
		return errors.New("error creating S3 client")
	}

	err = s.CheckBucket()

	if err != nil {
		slog.Error("Error checking S3 bucket", "error", err, "module", "writer.s3", "function", "Init")
		return err
	}

	return nil
}

func (s *S3) CheckBucket() error {
	_, err := s.client.HeadBucket(s.ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.config.S3BuketName),
	})

	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				slog.Info("S3 bucket not found", "module", "writer.s3", "function", "CheckBucket", "bucket", s.config.S3BuketName, "region", s.config.S3Region)
				err = nil
			}
		}
	} else {
		slog.Info("S3 bucket already exists", "module", "writer.s3", "function", "CheckBucket", "bucket", s.config.S3BuketName, "region", s.config.S3Region)
		return nil
	}

	ret, err := s.client.CreateBucket(s.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(s.config.S3BuketName),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(s.config.S3Region),
		},
	})

	if ret != nil {
		slog.Info("S3 bucket created", "module", "writer.s3", "function", "CheckBucket", "bucket", s.config.S3BuketName, "region", s.config.S3Region)
		return nil
	}

	if err != nil {
		slog.Warn("Warning to check S3 bucket", "error", err, "module", "writer.s3", "function", "CheckBucket", "bucket", s.config.S3BuketName, "region", s.config.S3Region)
	}

	return nil
}

func (s *S3) Write(key string, buf *bytes.Buffer) error {
	start := time.Now()

	_, err := s.client.PutObject(
		s.ctx,
		&s3.PutObjectInput{
			Bucket: aws.String(s.config.S3BuketName),
			Key:    aws.String(s.makeBuketName(key)),
			Body:   bytes.NewReader(buf.Bytes()),
		},
	)

	if err != nil {
		slog.Error("Error writing to S3", "error", err, "module", "writer.s3", "function", "Write", "key", key)
	}

	slog.Info("S3 written", "module", "writer.file", "function", "Write", "key", key, "duration", time.Since(start), "file-size", buf.Len(), "bucket", s.config.S3BuketName)

	return err
}

func (s *S3) makeBuketName(key string) string {
	tm := time.Now()
	year, month, day := tm.Date()
	hour, min, sec := tm.Clock()

	return fmt.Sprintf("%d/%02d/%02d/%02d%02d%02d-%s.parquet", year, month, day, hour, min, sec, key)
}

func (s *S3) Close() error {
	slog.Debug("Closing AWS-S3 writer")
	return nil
}

func (s *S3) IsReady() bool {
	return s.client != nil
}
