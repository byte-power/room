// This program is used to test redis transaction
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/pflag"
)

var key = pflag.StringP("key", "k", "{a}:counter", "tested key")

func main() {
	pflag.Parse()
	if key == nil {
		panic("key is not set")
	}
	transaction(*key)
}

var roomConfig = &redis.Options{
	Addr:         "localhost:6379",
	PoolSize:     50,
	ReadTimeout:  500 * time.Millisecond,
	WriteTimeout: 500 * time.Millisecond,
	DialTimeout:  500 * time.Millisecond,
	MinIdleConns: 50,
	PoolTimeout:  500 * time.Millisecond,
}

func transaction(key string) {
	const maxRetries = 1000
	ctx := context.TODO()
	client := redis.NewClient(roomConfig)

	// Increment transactionally increments key using GET and SET commands.
	increment := func(key string, index int) error {
		// Transactional function.
		txf := func(tx *redis.Tx) error {
			// Get current value or zero.
			n, err := tx.Get(ctx, key).Int()
			if err != nil && err != redis.Nil {
				return err
			}

			// Actual opperation (local in optimistic lock).
			n++

			// Operation is commited only if the watched keys remain unchanged.
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, key, n, 0)
				return nil
			})
			return err
		}

		for i := 0; i < maxRetries; i++ {
			startTime := time.Now()
			err := client.Watch(ctx, txf, key)
			endTime := time.Now()
			if err == nil {
				// Success.
				return nil
			}
			if err == redis.TxFailedErr {
				// Optimistic lock lost. Retry.
				fmt.Printf("goroutine %d lost lock,  has retried %d times,  retry again.\n", index, i)
				continue
			}
			if strings.Contains(err.Error(), "load key conflict") {
				fmt.Printf("goroutine %d loaded key conflict,  has retried %d times,  retry again.\n", index, i)
				continue
			}
			if strings.Contains(err.Error(), "i/o timeout") {
				fmt.Printf("goroutine %d timeout, has retried %d times, execution duration %v\n", index, i, endTime.Sub(startTime))
			}
			// Return any other error.
			return err
		}

		return errors.New("increment reached maximum number of retries")
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			if err := increment(key, index); err != nil {
				fmt.Println("increment error:", err)
			}
		}(i)
	}
	wg.Wait()

	n, err := client.Get(ctx, key).Int()
	fmt.Printf("key %s ended with %d, error is %v\n", key, n, err)
}
