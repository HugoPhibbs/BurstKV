package main

import (
	"burstkv/internal/core/redis_proxy"
	"burstkv/internal/core/snapshot_storage"
	"burstkv/internal/worker"
	"burstkv/internal/worker/grpc"
	"context"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	// Context to handle graceful shutdown on SIGINT or SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	storagePort := "9000"
	accessKey := "test-ak"
	secretKey := "test-sk"
	exec.CommandContext(ctx, "python", "scripts/start_dev_snapshot_server.py", "--api-port", storagePort, "--access-key", accessKey, "--secret-key", secretKey).Start()

	storageAddr := "localhost:" + storagePort
	minIoCreds := credentials.NewStaticV4(accessKey, secretKey, "")
	snapShotStorage := snapshot_storage.NewSnapshotStorage(storageAddr, "snapshots", minIoCreds, false)

	worker, err := worker.NewWorker("worker-1", 30000, 10, snapShotStorage)
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
	worker.StopAllContainers()
}
