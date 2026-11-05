package main

import (
	"burstkv/internal/core/redis_proxy"
	"burstkv/internal/core/snapshot_storage/snapshot_storage_test"
	"burstkv/internal/worker"
	"burstkv/internal/worker/grpc"
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Context to handle graceful shutdown on SIGINT or SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	worker, err := worker.NewWorker("worker-1", 30000, 10, &snapshot_storage_test.NoopSnapshotStorage{})
	if err != nil {
		log.Fatal(err)
	}

	proxy := redis_proxy.NewRedisProxy(":6380", worker)

	go func() {
		log.Default().Println("Redis proxy listening on :6380")
		if err := proxy.ListenAndServe(); err != nil {
			log.Println(err)
			stop()
		}
	}()

	go func() {
		log.Println("worker gRPC listening on :50051")
		if err := grpc.New(worker).ListenAndServe(":50051"); err != nil {
			log.Println(err)
			stop()
		}
	}()

	<-ctx.Done() // Wait for SIGINT or SIGTERM
	stop()       // A second ctrl+c or SIGTERM will exit immediately.
	log.Println("Shutting down gracefully... (press Ctrl+C again to force exit - NB: Not recommended)")
	worker.Close()
}
