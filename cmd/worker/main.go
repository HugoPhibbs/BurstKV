package main

import (
	"burstkv/internal/core/redis_proxy"
	"burstkv/internal/core/snapshot_storage"
	"burstkv/internal/worker"
	"burstkv/internal/worker/grpc"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"flag"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	// Context to handle graceful shutdown on SIGINT or SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	workerID := flag.String("id", "default", "Worker ID")
	proxyPort := flag.Int("proxy-port", 6380, "Redis Proxy port")
	grpcPort := flag.Int("grpc-port", 6381, "Worker GRPC port")
	snapshotStorageAddress := flag.String("storage-address", "localhost:9000", "Snapshot storage address")

	accessKey := flag.String("access-key", "default-ak", "Snapshot storage Access Key")
	secretKey := flag.String("secret-key", "default-sk", "Snapshot storage Secret Key")

	minContainerPort := flag.Int("min-port", 30000, "Minimum container port")
	poolSize := flag.Int("pool-size", 10, "Size of the container pool")

	flag.Parse()

	minIoCreds := credentials.NewStaticV4(*accessKey, *secretKey, "")
	snapShotStorage := snapshot_storage.NewSnapshotStorage(*snapshotStorageAddress, "snapshots", minIoCreds, false)

	worker, err := worker.NewWorker(ctx, *workerID, *minContainerPort, *poolSize, snapShotStorage)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		proxyAddr := fmt.Sprintf(":%d", *proxyPort)
		log.Default().Println("Redis proxy listening on " + proxyAddr)
		if err := redis_proxy.NewRedisProxy(proxyAddr, worker).ListenAndServe(); err != nil {
			log.Println(err)
			stop()
		}
	}()
		
	go func() {
		grpcAddr := fmt.Sprintf(":%d", *grpcPort)
		log.Println("worker gRPC listening on " + grpcAddr)
		if err := grpc.New(worker).ListenAndServe(grpcAddr); err != nil {
			log.Println(err)
			stop()
		}
	}()

	<-ctx.Done() // Wait for SIGINT or SIGTERM
	stop()       // A second ctrl+c or SIGTERM will exit immediately.
	log.Println("Shutting down gracefully... (press Ctrl+C again to force exit - NB: Not recommended)")
	worker.StopAllContainers(ctx)
}
