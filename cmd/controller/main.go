package main

import (
	"burstkv/internal/controller"
	"burstkv/internal/controller/grpc_client_factory"
	"burstkv/internal/controller/pod_manager"
	"burstkv/internal/controller/server"
	"burstkv/internal/core/redis_proxy"
	"burstkv/internal/core/snapshot_storage"
	"burstkv/internal/utils"
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

// "burstkv/internal/worker_manager"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverPort := flag.Int("server-port", 6378, "Server port")
	proxyPort := flag.Int("proxy-port", 6379, "Redis Proxy port")
	grpcPodPort := flag.Int("grpc-port", 6380, "gRPC port for each pod")

	idleLimit := flag.Int("idle-limit", 300, "Idle limit for any given database (seconds)")
	reaperInterval := flag.Int("reaper-interval", 60, "Interval time between sequential runs of the idle database reaper (seconds)")

	snapshotStorageAddress := flag.String("storage-address", "localhost:9000", "Snapshot storage address")
	accessKey := flag.String("access-key", "default-ak", "Snapshot storage Access Key")
	secretKey := flag.String("secret-key", "default-sk", "Snapshot storage Secret Key")

	flag.Parse()

	minIoCreds := credentials.NewStaticV4(*accessKey, *secretKey, "")
	snapshotStorage := snapshot_storage.NewSnapshotStorage(*snapshotStorageAddress, "snapshots", minIoCreds, false)

	toSecondsDuration := func(t *int) time.Duration { return time.Duration(*t) * time.Second }

	podManager, err := pod_manager.NewPodManager()
	if err != nil {
		panic(err)
	}

	grpcClientFactory := grpc_client_factory.NewGRPCClientFactory(*grpcPodPort)

	newController := controller.NewController(toSecondsDuration(idleLimit), toSecondsDuration(reaperInterval), snapshotStorage, *proxyPort, podManager, grpcClientFactory)

	go func() {
		proxyAddr := utils.PortToLocalAddr(*proxyPort)
		log.Default().Println("Redis proxy listening on " + proxyAddr)
		if err := redis_proxy.NewRedisProxy(proxyAddr, newController).ListenAndServe(ctx); err != nil {
			log.Println(err)
			stop()
		}
	}()

	go func() {
		serverAddr := utils.PortToLocalAddr(*serverPort)
		httpServer := server.New(newController)
		log.Default().Println("Server listening on " + serverAddr)
		if err := httpServer.Run(serverAddr); err != nil {
			log.Println(err)
			stop()
		}
	}()

	<-ctx.Done()
	stop()
	log.Println("Stopping gracefully...")

}
