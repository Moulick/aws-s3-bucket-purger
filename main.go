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
	Bucket     string
	Prefix     string
	Concurrent int
	S3pBinary  string
)

type container struct {
	mu       sync.Mutex
	counters map[string]int
}

func (c *container) inc(name string, amt int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counters[name] = c.counters[name] + amt
}

func (c *container) get(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counters[name]
}

const totalDiscovered = "totalDiscovered"
const totalDeleted = "totalDeleted"

func collectFiles(c *container, wg *sync.WaitGroup, s3ListQueue chan string) {

	// Create Cmd with options
	s3pCmd := cmd.NewCmdOptions(
		cmd.Options{
			Buffered:  false,
			Streaming: true,
		}, S3pBinary,
		"ls",
		"--bucket",
		Bucket,
		"--prefix",
		Prefix,
	)
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
				//time.Sleep(200 * time.Microsecond)
				s3ListQueue <- line       // Add the S3 key to the queue
				c.inc(totalDiscovered, 1) // Increment the total discovered counter
				//log.Println(line)
			case line, open := <-s3pCmd.Stderr:
				if !open {
					s3pCmd.Stderr = nil
					continue
				}
				log.Fatalln(line)
			}
		}
		close(s3ListQueue)
		log.Printf("s3p done, total keys discovered: %d", c.get(totalDiscovered))
	}(s3ListQueue)

	s3pCmd.Start()
	log.Printf("s3p started with arguments: %s", s3pCmd.Args)
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
	flag.IntVar(&Concurrent, "concurrent", 5, "Number of concurrent deletions to run")
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

	log.Printf("total discovered %d keys, total deleted %d keys", c.get(totalDiscovered), c.get(totalDeleted))
	log.Printf("finished cleaning bucket %s with prefix %s", Bucket, Prefix)
}

func s3Init(ctx context.Context) *s3.Client {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Create an Amazon S3 service s3Client
	s3Client := s3.NewFromConfig(cfg)
	return s3Client
}

func logPrinter(c *container) {
	for {
		log.Printf("discovered %d keys, deleted %d keys", c.get(totalDiscovered), c.get(totalDeleted))
		time.Sleep(500 * time.Millisecond)
	}
}

func deleteFiles(ctx context.Context, c *container, s3client *s3.Client, wg *sync.WaitGroup, input chan string) {
	routineCounter := container{counters: map[string]int{
		"routineNumber": 0,
	}}
	for i := 0; i < Concurrent; i++ {
		routineCounter.inc("routineNumber", 1)
		wg.Add(1)
		log.Printf("starting delete routine: %d", routineCounter.get("routineNumber"))
		go func(input chan string) {
			for keepGoing := true; keepGoing; {
				var objectIds []types.ObjectIdentifier
				expire := time.After(time.Second / 2) // The time after which we will send the delete request unless we have 1000 keys
				for {
					select {
					case key, ok := <-input:
						if !ok {
							keepGoing = false
							goto done
						}
						if key != "" {
							objectIds = append(objectIds, types.ObjectIdentifier{Key: aws.String(key)})
							//c.inc(totalDeleted, 1)
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
					}
					//time.Sleep(1000 * time.Millisecond)
					log.Printf("deleting keys count: %d", len(objectIds))
					c.inc(totalDeleted, len(objectIds))
				}
			}
			wg.Done()
			routineCounter.inc("routineNumber", -1)
		}(input)
	}
}
