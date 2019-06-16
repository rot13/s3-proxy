package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"net/http"
	"time"
)

type S3Proxy interface {
	Get(key string, header http.Header) (*s3.GetObjectOutput, error)
	GetWebsiteConfig() (*s3.GetBucketWebsiteOutput, error)
}

type RealS3Proxy struct {
	bucket string
	s3     *s3.S3
}

func NewS3Proxy(key, secret, region, bucket string) S3Proxy {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(key, secret, ""),
	}))

	return &RealS3Proxy{
		bucket: bucket,
		s3:     s3.New(sess),
	}
}

func (p *RealS3Proxy) Get(key string, header http.Header) (*s3.GetObjectOutput, error) {
	if_modified_since_time, err := http.ParseTime(header.Get("If-Modified-Since"))
	var if_modified_since_awstime *time.Time = nil
	if err == nil {
		if_modified_since_awstime = aws.Time(if_modified_since_time)
	}

	if_unmodified_since_time, err := http.ParseTime(header.Get("If-Unmodified-Since"))
	var if_unmodified_since_awstime *time.Time = nil
	if err == nil {
		if_unmodified_since_awstime = aws.Time(if_unmodified_since_time)
	}

	req := &s3.GetObjectInput{
		Bucket:            aws.String(p.bucket),
		Key:               aws.String(key),
		Range:             aws.String(header.Get("Range")),
		IfMatch:           aws.String(header.Get("If-Match")),
		IfNoneMatch:       aws.String(header.Get("If-None-Match")),
		IfModifiedSince:   if_modified_since_awstime,
		IfUnmodifiedSince: if_unmodified_since_awstime,
	}

	return p.s3.GetObject(req)
}

func (p *RealS3Proxy) GetWebsiteConfig() (*s3.GetBucketWebsiteOutput, error) {
	req := &s3.GetBucketWebsiteInput{
		Bucket: aws.String(p.bucket),
	}

	return p.s3.GetBucketWebsite(req)
}
