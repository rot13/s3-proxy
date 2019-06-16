package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"net/http"
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
	req := &s3.GetObjectInput{
		Bucket:      aws.String(p.bucket),
		Key:         aws.String(key),
		Range:       aws.String(header.Get("Range")),
		IfMatch:     aws.String(header.Get("If-Match")),
		IfNoneMatch: aws.String(header.Get("If-None-Match")),
	}

	return p.s3.GetObject(req)
}

func (p *RealS3Proxy) GetWebsiteConfig() (*s3.GetBucketWebsiteOutput, error) {
	req := &s3.GetBucketWebsiteInput{
		Bucket: aws.String(p.bucket),
	}

	return p.s3.GetBucketWebsite(req)
}
