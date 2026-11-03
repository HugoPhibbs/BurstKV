package main

import (
	"time"
	"burstkv/internal/db_manager"
	"burstkv/internal/local_db_runtime"
	"burstkv/internal/manager_server"
	"burstkv/internal/db_proxy"
)

func main() {
	runtime := local_db_runtime.NewLocalDBRuntime(30000, 30010)
	dbManager := db_manager.NewDBManager(runtime, 5*time.Minute)

	proxy := db_proxy.NewDBProxy(":6379", dbManager)
	go func() {
		if err := proxy.ListenAndServe(); err != nil {
			panic(err)
		}
	}()
	router := manager_server.New(dbManager)
	router.Run(":8080")
}
