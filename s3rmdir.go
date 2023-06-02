package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type objectVersion struct {
	Key       string
	VersionId string
}

type deleteBatchResult struct {
	BatchSize  int
	ErrorCount int
}

func deleteObjectVersions(
	resultChannel chan deleteBatchResult,
	client *s3.Client,
	bucket string,
	objectVersions []objectVersion,
) {
	deleteParam := &types.Delete{
		Objects: make([]types.ObjectIdentifier, 0, len(objectVersions)),
		Quiet:   true,
	}
	for _, v := range objectVersions {
		deleteParam.Objects = append(deleteParam.Objects, types.ObjectIdentifier{
			Key:       aws.String(v.Key),
			VersionId: aws.String(v.VersionId),
		})
	}
	params := s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: deleteParam,
	}
	result, err := client.DeleteObjects(context.TODO(), &params)
	if err != nil {
		log.Fatalf("failed to delete objects: %v", err)
	}
	resultChannel <- deleteBatchResult{
		BatchSize:  len(objectVersions),
		ErrorCount: len(result.Errors),
	}
}

func main() {
	fPrefix := flag.String("prefix", "", "`prefix`/folder to delete")
	fBucket := flag.String("bucket", "", "`bucket` to delete from (required)")
	fBatchSize := flag.Uint("batch", 1000, "batch size")
	fRegion := flag.String("region", "eu-west-1", "AWS `region`")

	flag.Parse()

	prefix := strings.Trim(*fPrefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	if *fBucket == "" {
		flag.Usage()
		os.Exit(1)
	}
	if *fBatchSize > math.MaxInt {
		log.Fatal("illegal batch size")
	}
	batchSize := int(*fBatchSize)

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(*fRegion),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	results := make(chan deleteBatchResult, 1000)
	var waitGroup sync.WaitGroup

	s3Client := s3.NewFromConfig(cfg)
	listObjectVersionsParams := s3.ListObjectVersionsInput{
		Bucket: fBucket,
		Prefix: aws.String(prefix),
	}
	objectPaginator := s3.NewListObjectVersionsPaginator(s3Client, &listObjectVersionsParams)
	batch := make([]objectVersion, 0, batchSize)

	numObjects := 0

	go func() {
		numProcessed := 0
		numErrors := 0
		for r := range results {
			numProcessed += r.BatchSize
			numErrors += r.ErrorCount
			fmt.Printf("%d objects deleted, %d errors\n", numProcessed, numErrors)
			waitGroup.Done()
		}
	}()

	for objectPaginator.HasMorePages() {
		page, err := objectPaginator.NextPage(context.TODO())
		if err != nil {
			log.Fatalf("failed to list objects: %v", err)
		}
		deleteVersion := func(key, versionId string) {
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				log.Fatalf("encountered object without requested prefix: %s", key)
			}
			batch = append(batch, objectVersion{
				Key:       key,
				VersionId: versionId,
			})
			numObjects++
			if len(batch) == batchSize {
				waitGroup.Add(1)
				go deleteObjectVersions(results, s3Client, *fBucket, batch)
				batch = make([]objectVersion, 0, batchSize)
			}
		}
		for _, v := range page.Versions {
			deleteVersion(*v.Key, *v.VersionId)
		}
		for _, v := range page.DeleteMarkers {
			deleteVersion(*v.Key, *v.VersionId)
		}
	}
	if len(batch) > 0 {
		waitGroup.Add(1)
		go deleteObjectVersions(results, s3Client, *fBucket, batch)
	}
	waitGroup.Wait()
	fmt.Printf("total number of objects: %d", numObjects)
}
