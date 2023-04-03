package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-cmd/cmd"
	"github.com/jnovack/flag"
)

var (
	Bucket      string
	Prefix      string
	Concurrency int
	S3pBinary   string
	startTime   time.Time
)

type container struct {
	mu       sync.Mutex
	counters map[string]int
}

// Increments the counter for the given name by 1.
func (c *container) inc(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counters[name]++
}

// Decrements the counter for the given name by 1.
func (c *container) dec(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counters[name]--
}

// Changes the counter for the given name by given amount.
func (c *container) add(name string, amt int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counters[name] = c.counters[name] + amt
}

// Returns the current value of the counter for the given name.
func (c *container) get(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counters[name]
}

const (
	totalDiscovered = "totalDiscovered"
	totalDeleted    = "totalDeleted"
	threadNumber    = "threadNumber"
)

// Returns a s3.Client with the default configuration
func s3Init(ctx context.Context) *s3.Client {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRetryMode(aws.RetryModeAdaptive), // try the new Adaptive retry mode
		config.WithRetryMaxAttempts(5),
		config.WithLogConfigurationWarnings(true),
		config.WithS3UseARNRegion(true), // Use the region from the A
	)
	if err != nil {
		log.Fatal(err)
	}

	// Create an Amazon S3 service s3Client
	s3Client := s3.NewFromConfig(cfg)
	return s3Client
}

// Continuously prints the current state of the counters
func logPrinter(c *container) {
	startTime = time.Now()
	for {
		log.Printf("discovered %d keys, deleted %d keys in %s", c.get(totalDiscovered), c.get(totalDeleted), time.Since(startTime).Round(time.Second))
		time.Sleep(500 * time.Millisecond)
	}
}

func collectFiles(c *container, wg *sync.WaitGroup, s3ListQueue chan string) {
	// Create Cmd with options
	s3pCmd := cmd.NewCmdOptions(
		cmd.Options{
			Buffered:  false,
			Streaming: true,
		}, S3pBinary, "ls", "--bucket", Bucket)
	if Prefix != "" {
		s3pCmd.Args = append(s3pCmd.Args, "--prefix", Prefix)
	}
	//s3pCmd := cmd.NewCmdOptions(
	//	cmd.Options{
	//		Buffered:  false,
	//		Streaming: true,
	//	}, "cat",
	//	"./sample.txt",
	//)
	s3pCmd.Env = os.Environ()
	// Print STDOUT and STDERR lines streaming from Cmd
	wg.Add(1)
	go func(s3ListQueue chan string) {
		defer wg.Done()
		// Done when both channels have been closed
		// https://dave.cheney.net/2013/04/30/curious-channels
		for s3pCmd.Stdout != nil || s3pCmd.Stderr != nil {
			select {
			case line, open := <-s3pCmd.Stdout:
				if !open {
					s3pCmd.Stdout = nil
					continue
				}
				// time.Sleep(200 * time.Microsecond)
				s3ListQueue <- line    // Add the S3 key to the queue
				c.inc(totalDiscovered) // Increment the total discovered counter
				// log.Println(line)
			case line, open := <-s3pCmd.Stderr:
				if !open {
					s3pCmd.Stderr = nil
					continue
				}
				log.Fatalln(line)
			}
		}
		log.Printf("s3p done, total keys discovered: %d", c.get(totalDiscovered))
		if c.get(totalDiscovered) == 0 {
			log.Println("no keys to delete, exiting")
			os.Exit(0)
		}
	}(s3ListQueue)

	s3pCmd.Start()
	log.Printf("s3p started with arguments: %s", s3pCmd.Args)
}

func deleteFiles(ctx context.Context, c *container, s3client *s3.Client, wg *sync.WaitGroup, s3ListQueue chan string) {
	threadCounter := container{counters: map[string]int{
		threadNumber: 0,
	}}
	for i := 0; i < Concurrency; i++ {
		threadCounter.inc(threadNumber)
		wg.Add(1)
		log.Printf("starting delete thread: %d", threadCounter.get(threadNumber))
		go func(s3ListQueue chan string) {
			for keepGoing := true; keepGoing; {
				var objectIds []types.ObjectIdentifier
				expire := time.After(time.Second / 2) // The time after which we will send the delete request unless we have 1000 keys
				for {
					select {
					case key, ok := <-s3ListQueue:
						if !ok {
							keepGoing = false
							goto done
						}
						if key != "" {
							objectIds = append(objectIds, types.ObjectIdentifier{Key: aws.String(key)})
							// c.inc(totalDeleted, 1)
						}
						if len(objectIds) == 1000 {
							goto done
						}
					case <-expire:
						goto done
					}
				}
			done:
				if len(objectIds) > 0 {
					_, err := s3client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
						Bucket: aws.String(Bucket),
						Delete: &types.Delete{Objects: objectIds, Quiet: true},
					})
					if err != nil {
						log.Println(err)
						for _, objectId := range objectIds {
							s3ListQueue <- *objectId.Key // adding the keys back to the queue
						}
					}
					// time.Sleep(1000 * time.Millisecond)
					log.Printf("deleting keys count: %d", len(objectIds))
					c.add(totalDeleted, len(objectIds))
				}
			}
			wg.Done()
			threadCounter.dec("routineNumber")
		}(s3ListQueue)
	}
}

// Deletes data from bucket including versions
// Example usage would be as follows
// AwsRegion=ap-southeast-2 S3Bucket=my.bucket S3Prefix=191 go run main.go
// Which will load delete all records with the bucket with the prefix 191
func main() {
	ctx := context.Background()

	var wg sync.WaitGroup
	s3ListQueue := make(chan string, 1000000)
	c := container{
		counters: map[string]int{
			totalDiscovered: 0,
			totalDeleted:    0,
		},
	}

	flag.StringVar(&Bucket, "bucket_name", "", "S3 bucket name")
	flag.StringVar(&Prefix, "prefix", "", "S3 prefix/folder to delete")
	flag.StringVar(&S3pBinary, "s3pBinary", "./s3p", "Path to the s3p Binary")
	flag.IntVar(&Concurrency, "concurrency", 5, "Number of concurrent deletions to run")
	flag.Parse()
	if Bucket == "" {
		log.Fatalln("Bucket Name cannot be empty")
	}

	go logPrinter(&c) // Start a log printer to print the current status of the process
	log.Printf("starting deleting files from bucket %s with prefix %s", Bucket, Prefix)

	s3client := s3Init(ctx)

	collectFiles(&c, &wg, s3ListQueue)
	deleteFiles(ctx, &c, s3client, &wg, s3ListQueue)

	wg.Wait() // Wait for all the go routines to finish
	close(s3ListQueue)

	log.Printf("total discovered %d keys, total deleted %d keys in %s", c.get(totalDiscovered), c.get(totalDeleted), time.Since(startTime))
	log.Printf("finished cleaning bucket %s with prefix %s", Bucket, Prefix)
}
