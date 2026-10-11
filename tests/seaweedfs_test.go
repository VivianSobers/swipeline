package tests

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestSeaweedFSS3Compatibility(t *testing.T) {
	endpoint := os.Getenv("SWIPELINE_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("run make m4-seaweedfs-smoke")
	}

	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("any", "any", ""),
		UsePathStyle: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bucket := "swipeline"

	// 1. Create bucket swipeline if absent
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		var alreadyExists *s3types.BucketAlreadyExists
		var alreadyOwned *s3types.BucketAlreadyOwnedByYou
		if !errors.As(err, &alreadyExists) && !errors.As(err, &alreadyOwned) && !strings.Contains(err.Error(), "BucketAlreadyExists") {
			t.Fatalf("failed to create bucket %s: %v", bucket, err)
		}
	}

	// 2. PutObject ledger/test.txt
	key := "ledger/test.txt"
	content := fmt.Sprintf("swipeline lakehouse test payload %s", time.Now().UTC().Format(time.RFC3339Nano))
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(content)),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// 3. GetObject and verify content
	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	gotData, err := io.ReadAll(getOut.Body)
	getOut.Body.Close()
	if err != nil {
		t.Fatalf("reading GetObject body: %v", err)
	}
	if string(gotData) != content {
		t.Fatalf("content mismatch: got %q, want %q", string(gotData), content)
	}

	// 4. ListObjectsV2
	listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("ledger/"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}
	found := false
	for _, obj := range listOut.Contents {
		if aws.ToString(obj.Key) == key {
			found = true
			if obj.Size == nil || *obj.Size != int64(len(content)) {
				t.Fatalf("object size mismatch: got %v, want %d", obj.Size, len(content))
			}
			break
		}
	}
	if !found {
		t.Fatalf("ListObjectsV2 did not find key %s", key)
	}

	// 5. Initiate multipart upload and abort
	mpKey := "ledger/multipart-abort-test.txt"
	createMpOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(mpKey),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}
	uploadID := createMpOut.UploadId
	if uploadID == nil || *uploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}

	_, err = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(mpKey),
		UploadId: uploadID,
	})
	if err != nil {
		t.Fatalf("AbortMultipartUpload failed: %v", err)
	}

	// 6. Verify read-after-write consistency
	for i := 0; i < 5; i++ {
		consistencyKey := fmt.Sprintf("ledger/consistency-%d.txt", i)
		consistencyData := fmt.Sprintf("consistency-check-%d-%d", i, time.Now().UnixNano())
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(consistencyKey),
			Body:   bytes.NewReader([]byte(consistencyData)),
		})
		if err != nil {
			t.Fatalf("PutObject consistency %d failed: %v", i, err)
		}
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(consistencyKey),
		})
		if err != nil {
			t.Fatalf("read-after-write GetObject %d failed: %v", i, err)
		}
		readBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("reading body %d: %v", i, err)
		}
		if string(readBody) != consistencyData {
			t.Fatalf("read-after-write consistency failure on iteration %d: got %q, want %q", i, string(readBody), consistencyData)
		}
	}
}
