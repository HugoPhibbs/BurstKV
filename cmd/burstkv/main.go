package main

import (
	"time"
	"burstkv/internal/db_manager"
	"burstkv/internal/local_db_runtime"
	"burstkv/internal/manager_server"
)

func main() {
	runtime := local_db_runtime.NewLocalDBRuntime(30000, 30010)
	dbManager := db_manager.NewDBManager(runtime, 5*time.Minute)

	router := manager_server.New(dbManager)
	router.Run(":8080")
}
