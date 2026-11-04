package main

import (
	"burstkv/internal/core/redis_proxy"
	"burstkv/internal/worker_node/worker_grpc_server"
	"burstkv/internal/worker_node/worker_manager"
	"burstkv/internal/worker_node/worker_redis_router"
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

	wm, err := worker_manager.NewWorkerManager("worker-1", 30000, 10)
	if err != nil {
		log.Fatal(err)
	}

	proxy := redis_proxy.NewRedisProxy(":6380", worker_redis_router.NewWorkerRedisRouter(wm))

	go func() {
		log.Default().Println("Redis proxy listening on :6380")
		if err := proxy.ListenAndServe(); err != nil {
			log.Println(err)
			stop()
		}
	}()

	go func() {
		log.Println("worker gRPC listening on :50051")
		if err := worker_grpc_server.New(wm).ListenAndServe(":50051"); err != nil {
			log.Println(err)
			stop()
		}
	}()

	<-ctx.Done() // Wait for SIGINT or SIGTERM
	stop()       // A second ctrl+c or SIGTERM will exit immediately.
	log.Println("Shutting down gracefully... (press Ctrl+C again to force exit - NB: Not recommended)")
	wm.Close()
}
