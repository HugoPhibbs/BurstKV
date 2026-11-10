package controller

import (
	"burstkv/internal/controller/domain/database"
	"burstkv/internal/controller/pod_manager"
	"burstkv/internal/core"
	"burstkv/internal/core/snapshot_storage"
	"burstkv/internal/db_pod/db_pod_pb"
	"burstkv/internal/utils"
	"context"
	"fmt"
	"log"
	"maps"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const DB_ID_LENGTH = 8

type PodPorts struct {
	rpcPort   string
	proxyPort string
}

type Controller struct {
	idleLimit          time.Duration
	ReaperTimeInterval time.Duration
	podManager         *pod_manager.PodManager
	databases          map[string]*database.Database // databaseID -> Database
	snapShotstorage    *snapshot_storage.SnapshotStorage
	podPorts           *PodPorts
}

// Creates a new DBManager with the given runtime and idle limit
func NewController(idleLimit, reaperInterval time.Duration, snapshotStorage *snapshot_storage.SnapshotStorage, podPorts *PodPorts, podManager *pod_manager.PodManager) (*Controller, error) {
	return &Controller{
		idleLimit:          idleLimit,
		ReaperTimeInterval: reaperInterval,
		podManager:         podManager,
		databases:          make(map[string]*database.Database),
		snapShotstorage:    snapshotStorage,
		podPorts:           podPorts,
	}, nil
}

// Returns the state of the database with the given ID.
func (c *Controller) GetDatabaseState(databaseID string) (database.DatabaseStatus, error) {
	foundDatabase, exists := c.databases[databaseID]
	if !exists {
		return database.NotFound, nil
	}
	if foundDatabase.IsRunning() {
		return database.Running, nil
	}
	return database.Paused, nil
}

// Returns a list of all database IDs managed by this DBManager.
func (c *Controller) GetAllDatabaseIDs() []string {
	return slices.Collect(maps.Keys(c.databases))
}

// Deletes the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist.
func (c *Controller) DeleteDatabase(ctx context.Context, databaseID string) error {
	foundDB, exists := c.databases[databaseID]

	if !exists {
		return core.ErrDatabaseNotFound
	}

	if foundDB.IsRunning() {
		if err := c.stopDatabase(ctx, foundDB, false); err != nil {
			return err
		}
	}

	if err := c.snapShotstorage.DeleteSnapshot(ctx, databaseID); err != nil {
		log.Println(fmt.Sprintf("Error occured when deleting a snapshot %v", err))
	}

	delete(c.databases, databaseID)

	return nil
}

// Deletes all databases managed by this DBManager.
func (c *Controller) DeleteAllDatabases(ctx context.Context) error {
	for id := range c.databases {
		err := c.DeleteDatabase(ctx, id)
		if err != nil {
			return err
		}
	}
	return nil
}

// Pauses the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist or ErrDatabaseAlreadyPaused if the database is already paused.
func (c *Controller) PauseDatabase(ctx context.Context, databaseID string) error {
	foundDB, exists := c.databases[databaseID]
	if !exists {
		return core.ErrDatabaseNotFound
	}
	if !foundDB.IsRunning() {
		return core.ErrDatabaseAlreadyPaused
	}
	return c.stopDatabase(ctx, foundDB, true)
}

func (c *Controller) stopDatabase(ctx context.Context, foundDB *database.Database, save bool) error {
	client, err := c.databaseRPClient(foundDB.Pod.PodIP)
	if err != nil {
		return err
	}

	_, err = client.Stop(ctx, &db_pod_pb.StopRequest{Save: save})

	if err != nil {
		return err
	}

	foundDB.Pod = nil
	return nil
}

// Resumes the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist or ErrDatabaseAlreadyActive if the database is already active.
func (c *Controller) ResumeDatabase(ctx context.Context, databaseID string) error {
	foundDB, exists := c.databases[databaseID]

	if !exists {
		return core.ErrDatabaseNotFound
	}

	return c.assignDatabase(ctx, foundDB, false)
}

// Creates a new database and returns the new database ID
func (c *Controller) CreateDatabase(ctx context.Context) (string, error) {
	newDatabaseID := utils.RandomString(DB_ID_LENGTH)
	newDB := database.NewDatabase(newDatabaseID)

	if err := c.assignDatabase(ctx, newDB, true); err != nil {
		return "", err
	}

	c.databases[newDatabaseID] = newDB
	return newDatabaseID, nil
}

func (c *Controller) assignDatabase(ctx context.Context, DB *database.Database, create bool) error {
	if DB.IsRunning() {
		return core.ErrDatabaseAlreadyActive
	}

	claimedPod, err := c.podManager.ClaimWarmPod(ctx)
	if err != nil {
		return err
	}

	DB.Pod = claimedPod
	client, err := c.databaseRPClient(DB.Pod.PodIP)
	if err != nil {
		return err
	}

	_, err = client.Assign(ctx, &db_pod_pb.AssignDatabaseRequest{DatabaseId: DB.ID(), Create: create})
	return err
}

func (c *Controller) databaseRPClient(databasePodIP string) (db_pod_pb.DBPodClient, error) {
	conn, err := grpc.NewClient(databasePodIP+":"+c.podPorts.rpcPort,
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, err
	}

	client := db_pod_pb.NewDBPodClient(conn)
	return client, nil
}

func (c *Controller) reapIdleDatabases(ctx context.Context) {
	for _, db := range c.databases {
		if db.IsActiveAndIdle(c.idleLimit) {
			err := c.PauseDatabase(ctx, db.ID())
			if err != nil {
				fmt.Printf("Error pausing idle database %s: %v\n", db.ID(), err)
			}
		}
	}
}

func (c *Controller) StartIdleDatabaseReaper(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(c.ReaperTimeInterval)
		defer ticker.Stop()

		for range ticker.C {
			c.reapIdleDatabases(ctx)
		}
	}()
}

func (c *Controller) RouteDatabase(ctx context.Context, databaseID string) (string, error) {
	database, exists := c.databases[databaseID]
	if !exists {
		return "", core.ErrDatabaseNotFound
	}

	if !database.IsRunning() {
		if err := c.ResumeDatabase(ctx, databaseID); err != nil {
			return "", err
		}
	}

	return database.Pod.PodIP, nil
}

func (c *Controller) RecordDatabaseActivity(databaseID string) error {
	database, exists := c.databases[databaseID]
	if !exists {
		return core.ErrDatabaseNotFound
	}
	database.RecordActivity()
	return nil
}

func (c *Controller) StopAllWorkers() error {
	return nil // Implement me
}
