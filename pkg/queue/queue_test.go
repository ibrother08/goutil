package queue

import (
	"context"
	"fmt"
	"log"
	"sync"
)

func Example() {
	qe, err := NewQueue()
	if err != nil {
		log.Fatalf("err: %v\n", err)
	}

	ctx := context.Background()

	qe.Purge(ctx)

	var wg sync.WaitGroup
	wg.Add(1)

	if err = qe.Subscribe(ctx, "create", func(msg []byte) error {
		defer wg.Done()

		fmt.Println("consume message: " + string(msg))

		return nil
	}, WithDurable(false)); err != nil {
		log.Fatalf("err: %v\n", err)
	}

	if err = qe.Publish(ctx, "create", []byte("order number 1001")); err != nil {
		log.Fatalf("err: %v\n", err)
	}

	wg.Wait()

	// output: consume message: order number 1001
}
