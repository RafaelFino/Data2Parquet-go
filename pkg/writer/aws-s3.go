package writer

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"

	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
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

	slog.Info("Creating S3 writer")

	return ret
}

func (s *S3) Init() error {
	endpoints := map[string]string{
		"S3":  s.config.S3Endpoint,
		"STS": s.config.S3STSEndpoint,
	}

	slog.Debug("Initializing S3 writer", "config", s.config.ToJSON(), "endpoints", endpoints)

	cfg, err := awsConfig.LoadDefaultConfig(s.ctx,
		awsConfig.WithRegion(s.config.S3Region),
		awsConfig.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
			if endpoint, ok := endpoints[service]; ok {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           endpoint,
					SigningRegion: s.config.S3Region,
				}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})),
	)

	if err != nil {
		slog.Error("Error loading AWS config", "error", err, "module", "writer.s3", "function", "Init")
		return err
	}

	session := filepath.Base(s.config.S3RoleARN)
	slog.Info("Assuming role", "roleArn", s.config.S3RoleARN, "sessionName", session)

	stsClient := sts.NewFromConfig(cfg)

	cfg.Credentials = aws.NewCredentialsCache(
		stscreds.NewAssumeRoleProvider(stsClient, s.config.S3RoleARN, func(aro *stscreds.AssumeRoleOptions) {
			aro.RoleSessionName = session
		}),
	)

	slog.Debug("Get credentials, trying to create a S3 client")

	s.client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	if s.client == nil {
		slog.Error("Error creating S3 client", "module", "writer.s3", "function", "Init")
		return errors.New("error creating S3 client")
	}

	slog.Debug("S3 client created, checking bucket")

	err = s.CheckBucket()

	if err != nil {
		slog.Error("Error checking S3 bucket", "error", err, "module", "writer.s3", "function", "Init")
		return err
	}

	slog.Info("S3 writer initialized")

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
				slog.Info("S3 bucket not found", "bucket", s.config.S3BuketName, "region", s.config.S3Region)
			default:
				slog.Debug("AWS SDK return on checking S3 bucket", "return", err, "bucket", s.config.S3BuketName)
			}
		}
	} else {
		slog.Info("S3 bucket already exists", "bucket", s.config.S3BuketName, "region", s.config.S3Region)
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

	return err
}

func (s *S3) Write(key string, buf *bytes.Buffer) error {
	start := time.Now()
	recInfo := domain.NewRecordInfoFromKey(s.config.RecordType, key)
	id := domain.MakeID()
	var hash = ""
	if s.config.UseHash {
		hash = "-" + domain.GetMD5Sum(buf.Bytes())
	}
	s3Key := recInfo.Target(id, hash)

	_, err := s.client.PutObject(
		s.ctx,
		&s3.PutObjectInput{
			Bucket: aws.String(s.config.S3BuketName),
			Key:    aws.String(s3Key),
			Body:   bytes.NewReader(buf.Bytes()),
		},
	)

	if err != nil {
		slog.Error("Error writing to S3", "error", err, "module", "writer.s3", "function", "Write", "key", key)
		return err
	}

	slog.Info("S3 written", "file", s3Key, "duration", time.Since(start), "file-size", buf.Len(), "bucket", s.config.S3BuketName)

	return nil
}

func (s *S3) Close() error {
	slog.Debug("Closing AWS-S3 writer")
	return nil
}

func (s *S3) IsReady() bool {
	return s.client != nil
}
