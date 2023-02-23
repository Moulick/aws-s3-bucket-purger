package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-cmd/cmd"
)

type VersionKey struct {
	Name      string
	VersionId string
}

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

// Deletes data from bucket including versions
// Example usage would be as follows
// AwsRegion=ap-southeast-2 S3Bucket=my.bucket S3Prefix=191 go run main.go
// Which will load delete all records with the bucket with the prefix 191
func main() {
	purge()
}

func purge() {
	svc, err := session.NewSession(&aws.Config{
		Region: aws.String(getEnvString("AwsRegion", ""))},
	)
	if err != nil {
		log.Println(err)
		return
	}
	_ = s3.New(svc)

	var wg sync.WaitGroup
	s3ListQueue := make(chan string, 99999)
	c := container{
		counters: map[string]int{
			totalDiscovered: 0,
			totalDeleted:    0,
		},
	}

	// Create Cmd with options
	s3pCmd := cmd.NewCmdOptions(
		cmd.Options{
			Buffered:  false,
			Streaming: true,
		}, "./s3p-macos-arm64",
		"ls",
		"--bucket",
		"it-11613-druid-backup",
		"--prefix",
		"druid_pqai",
	)
	// Print STDOUT and STDERR lines streaming from Cmd
	doneChan := make(chan struct{})
	wg.Add(1)
	go func() {
		defer close(doneChan)
		// Done when both channels have been closed
		// https://dave.cheney.net/2013/04/30/curious-channels
		for s3pCmd.Stdout != nil || s3pCmd.Stderr != nil {
			select {
			case line, open := <-s3pCmd.Stdout:
				if !open {
					s3pCmd.Stdout = nil
					continue
				}
				log.Println(line)
				s3ListQueue <- line
			case line, open := <-s3pCmd.Stderr:
				if !open {
					s3pCmd.Stderr = nil
					continue
				}
				log.Println(os.Stderr, line)
			}
		}
	}()
	close(s3ListQueue)
	wg.Done()

	for i := 0; i < getEnvInt("LoadConcurrency", 1); i++ {
		wg.Add(1)
		go func(input chan string) {
			for keepGoing := true; keepGoing; {
				objects := s3.Delete{}
				expire := time.After(2 * time.Second)
				for {
					select {
					case key, ok := <-input:
						if !ok {
							keepGoing = false
							goto done
						}
						if key != "" {
							objects.Objects = append(objects.Objects, &s3.ObjectIdentifier{
								Key: aws.String(key),
							})
						}
						c.inc(totalDeleted, 1)
						if len(objects.Objects) == 1000 {
							goto done
						}
					case <-expire:
						goto done
					}
				}
			done:
				//_, err := s3client.DeleteObjects(&s3.DeleteObjectsInput{
				//	Bucket: aws.String(getEnvString("S3Bucket", "")),
				//	Delete: objects,
				//})
				time.Sleep(10 * time.Millisecond)
				log.Printf("deleting %d keys", len(objects.Objects))
				if err != nil {
					log.Println(err)
				}
			}
			wg.Done()
		}(s3ListQueue)
	}

	go func() {
		for {
			log.Println(fmt.Sprintf("Discovered %d keys, deleted %d keys", c.get(totalDiscovered), c.get(totalDeleted)))
			time.Sleep(100 * time.Millisecond)
		}
	}()

	log.Println(fmt.Sprintf("starting::bucket:%s::prefix:%s", getEnvString("S3Bucket", ""), getEnvString("S3Prefix", "")))
	// Run and wait for Cmd to return, discard Status
	s3pCmd.Start()
	wg.Wait()
	log.Println(fmt.Sprintf("Total Discovered %d keys, Total deleted %d keys",
		c.get(totalDiscovered),
		c.get(totalDeleted),
	))
	log.Println(fmt.Sprintf("finished::bucket:%s::prefix:%s", getEnvString("S3Bucket", ""), getEnvString("S3Prefix", "")))

	// Wait for goroutine to print everything
	<-doneChan

}

func getEnvString(variable string, def string) string {
	val := os.Getenv(variable)
	if val != "" {
		return val
	}
	return def
}

func getEnvInt(variable string, def int) int {
	tmp := os.Getenv(variable)
	if tmp != "" {
		val, err := strconv.Atoi(tmp)
		if err == nil {
			return val
		}
	}
	return def
}
