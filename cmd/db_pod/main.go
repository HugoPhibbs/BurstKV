package main

import (
	"burstkv/internal/db_pod"
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"burstkv/internal/db_pod/controller_grpc_client_factory"
	"burstkv/internal/db_pod/db_pod_grpc"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	grpcPort, ok := os.LookupEnv("GRPC_PORT")
	if !ok {
		log.Fatal("GRPC_PORT not set")
	}
	
	REDIS_PORT, ok := os.LookupEnv("REDIS_PORT")
	if !ok {
		log.Fatal("REDIS_PORT not set")
	}

	redisPort, err := strconv.Atoi(REDIS_PORT)
	if err != nil {
		panic(err)
	}

	controllerGRPCAddress, ok := os.LookupEnv("CONTROLLER_GRPC_ADDRESS")
	if !ok {
		log.Fatal("CONTROLLER_GRPC_ADDRESS not set")
	}

	controllerClientFactory := controller_grpc_client_factory.NewControllerGRPCClientFactory(controllerGRPCAddress)
	db := db_pod.NewDBPod(redisPort, controllerClientFactory)

	go func() {
		grpcAddr := ":" + grpcPort
		log.Println("pod gRPC listening on " + grpcAddr)
		if err := db_pod_grpc.NewPodGRPCServer(db).ListenAndServe(grpcAddr); err != nil {
			log.Println(err)
			stop()
		}
	}()
	<- ctx.Done()
	stop() // Stop catching signals
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25 * time.Second)
	defer cancel()
	log.Println("Performing shutdown (gracefully)")
	if err = db.HandleTermination(shutdownCtx, true); err != nil {
		log.Println(err)
	}
}
