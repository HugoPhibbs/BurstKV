package main

import (
	"burstkv/internal/db_pod"
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"burstkv/internal/db_pod/db_pod_grpc"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	GRPC_PORT, ok := os.LookupEnv("GRPC_PORT")
	if !ok {
		log.Fatal("GRPC_PORT not set")
	}
	
	REDIS_PORT, ok := os.LookupEnv("REDIS_PORT")
	if !ok {
		log.Fatal("REDIS_PORT not set")
	}

	p, err := strconv.Atoi(REDIS_PORT)
	if err != nil {
		panic(err)
	}
	db := db_pod.NewDBPod(p)

	go func() {
		grpcAddr := ":" + GRPC_PORT
		log.Println("pod gRPC listening on " + grpcAddr)
		if err := db_pod_grpc.NewPodGRPCServer(db).ListenAndServe(grpcAddr); err != nil {
			log.Println(err)
			stop()
		}
	}()
	<- ctx.Done()
	stop()
	log.Println("Performing shutdown (gracefully)")
	db.Stop(ctx, true)
}
