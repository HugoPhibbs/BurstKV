package controller

import (
	"burstkv/internal/controller/database"
	"burstkv/internal/controller/pod"
	"burstkv/internal/core"
	"burstkv/internal/db_pod/db_pod_pb"
	"burstkv/internal/utils"
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

const DB_ID_LENGTH = 8

type PodManager interface {
	ClaimWarmPod(ctx context.Context) (*pod.Pod, error)
	DeletePod(ctx context.Context, podName string) error
}

type PodGRPCClientFactory interface {
	NewConnectedClient(databasePodIP string) (db_pod_pb.DBPodClient, func() error, error)
}

const SNAPSHOT_UPLOAD_URL_EXPIRY = 5 * time.Minute

type SnapshotStorage interface {
	DeleteSnapshot(ctx context.Context, databaseID string) error
	PresignedPutURL(ctx context.Context, databaseID string, expiry time.Duration) (string, error)
	PresignedGetURL(ctx context.Context, databaseID string, expiry time.Duration) (string, error)
}

type Controller struct {
	idleLimit          time.Duration
	ReaperTimeInterval time.Duration
	podManager         PodManager
	grpcClientFactory  PodGRPCClientFactory
	databases          *xsync.Map[string, *database.Database] // databaseID -> Database
	snapShotstorage    SnapshotStorage
	proxyPort          int
}

// Creates a new DBManager with the given runtime and idle limit
func NewController(
	idleLimit time.Duration,
	reaperInterval time.Duration,
	snapshotStorage SnapshotStorage,
	proxyPort int,
	podManager PodManager,
	grpcClientFactory PodGRPCClientFactory) *Controller {
	return &Controller{
		idleLimit:          idleLimit,
		ReaperTimeInterval: reaperInterval,
		podManager:         podManager,
		grpcClientFactory:  grpcClientFactory,
		databases:          xsync.NewMap[string, *database.Database](),
		snapShotstorage:    snapshotStorage,
		proxyPort:          proxyPort,
	}
}

// Returns the state of the database with the given ID.
func (c *Controller) GetDatabaseState(databaseID string) (database.DatabaseStatus, error) {
	foundDatabase, exists := c.databases.Load(databaseID)
	if !exists {
		return database.NotFound, nil
	}

	foundDatabase.Lock()
	defer foundDatabase.Unlock()

	if foundDatabase.IsDeleted() {
		return database.NotFound, nil
	}

	if foundDatabase.IsRunning() {
		return database.Running, nil
	}
	return database.Paused, nil
}

// Returns a list of all database IDs managed by this DBManager.
func (c *Controller) GetAllDatabaseIDs() []string {
	ids := make([]string, 0, c.databases.Size())
	for id := range c.databases.All() {
		ids = append(ids, id)
	}
	return ids
}

// Deletes the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist.
func (c *Controller) DeleteDatabase(ctx context.Context, databaseID string) error {
	foundDB, exists := c.databases.Load(databaseID)

	if !exists {
		return core.ErrDatabaseNotFound
	}

	foundDB.Lock()
	defer foundDB.Unlock()

	if foundDB.IsDeleted() {
		return core.ErrDatabaseNotFound
	}

	if foundDB.IsRunning() {
		if err := c.stopDatabase(ctx, foundDB, false); err != nil {
			return err
		}
	}

	if err := c.snapShotstorage.DeleteSnapshot(ctx, databaseID); err != nil {
		log.Printf("Error occured when deleting a snapshot %v", err)
	}

	foundDB.SetIsDeleted(true)
	c.databases.Delete(databaseID)

	return nil
}

// Deletes all databases managed by this DBManager.
func (c *Controller) DeleteAllDatabases(ctx context.Context) error {
	for id := range c.databases.All() {
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
	foundDB, exists := c.databases.Load(databaseID)
	if !exists {
		return core.ErrDatabaseNotFound
	}
	foundDB.Lock()
	defer foundDB.Unlock()

	if foundDB.IsDeleted() {
		return core.ErrDatabaseNotFound
	}

	if !foundDB.IsRunning() {
		return core.ErrDatabaseAlreadyPaused
	}
	return c.stopDatabase(ctx, foundDB, true)
}

func (c *Controller) stopDatabase(ctx context.Context, foundDB *database.Database, save bool) error {
	client, close, err := c.grpcClientFactory.NewConnectedClient(foundDB.Pod().IP())
	if err != nil {
		return err
	}
	defer close()

	_, err = client.Stop(ctx, &db_pod_pb.StopRequest{Save: save})

	if err != nil {
		return err
	}

	if err := c.podManager.DeletePod(ctx, foundDB.Pod().Name()); err != nil {
		return err
	}

	foundDB.SetPod(nil)
	return nil
}

// Resumes the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist or ErrDatabaseAlreadyActive if the database is already active.
func (c *Controller) ResumeDatabase(ctx context.Context, databaseID string) error {
	foundDB, exists := c.databases.Load(databaseID)

	if !exists {
		return core.ErrDatabaseNotFound
	}

	foundDB.Lock()
	defer foundDB.Unlock()

	if foundDB.IsRunning() {
		return core.ErrDatabaseAlreadyActive
	}

	return c.resumeDatabaseHelper(ctx, foundDB)
}

func (c *Controller) resumeDatabaseHelper(ctx context.Context, DB *database.Database) error {
	if DB.IsDeleted() {
		return core.ErrDatabaseNotFound
	}

	claimedPod, podClient, close, err := c.ClaimWarmPodClient(ctx)
	if err != nil {
		return err
	}
	defer close()

	getUrl, err := c.snapShotstorage.PresignedGetURL(ctx, DB.ID(), 5*time.Minute)
	if err != nil {
		return nil
	}

	_, err = podClient.ResumeDatabase(ctx, &db_pod_pb.ResumeDatabaseRequest{DatabaseId: DB.ID(), GetUrl: getUrl})

	if err != nil {
		return err
	}

	DB.SetPod(claimedPod)
	return nil
}

// Creates a new database and returns the new database ID
func (c *Controller) CreateDatabase(ctx context.Context) (string, error) {
	newDatabaseID := utils.RandomString(DB_ID_LENGTH)
	newDB := database.NewDatabase(newDatabaseID)

	claimedPod, podClient, close, err := c.ClaimWarmPodClient(ctx)
	if err != nil {
		return "", err
	}
	defer close()

	_, err = podClient.CreateDatabase(ctx, &db_pod_pb.CreateDatabaseRequest{DatabaseId: newDatabaseID})

	if err != nil {
		return "", err
	}

	newDB.SetPod(claimedPod)
	c.databases.Store(newDatabaseID, newDB)
	return newDatabaseID, nil
}

func (c *Controller) ClaimWarmPodClient(ctx context.Context) (*pod.Pod, db_pod_pb.DBPodClient, func() error, error) {
	claimedPod, err := c.podManager.ClaimWarmPod(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	client, close, err := c.grpcClientFactory.NewConnectedClient(claimedPod.IP())
	if err != nil {
		return nil, nil, nil, err
	}
	return claimedPod, client, close, nil
}

func (c *Controller) MigrateDatabase(ctx context.Context, databaseID string) error {
	primaryDB, ok := c.databases.Load(databaseID)
	if !ok {
		return core.ErrDatabaseNotFound
	}
	primaryDB.Lock()
	defer primaryDB.Unlock()

	if primaryDB.IsDeleted() {
		return core.ErrDatabaseNotFound
	}

	// Find a warm pod for the replica
	replicaPod, err := c.podManager.ClaimWarmPod(ctx)
	if err != nil {
		return err
	}

	// Replicate
	replicaClient, closeReplica, err := c.grpcClientFactory.NewConnectedClient(replicaPod.IP())
	if err != nil {
		return err
	}
	defer closeReplica()

	primaryAddress := net.JoinHostPort(primaryDB.Pod().IP(), strconv.Itoa(c.proxyPort))
	_, err = replicaClient.ReplicateDatabase(ctx, &db_pod_pb.ReplicateDatabaseRequest{DatabaseId: databaseID, PrimaryAddress: primaryAddress})
	if err != nil {
		return err
	}

	// Migrate
	primaryClient, closePrimary, err := c.grpcClientFactory.NewConnectedClient(primaryDB.Pod().IP())
	if err != nil {
		return err
	}
	defer closePrimary()

	replicaAddress := net.JoinHostPort(replicaPod.IP(), strconv.Itoa(c.proxyPort))
	_, err = primaryClient.MigrateDatabase(ctx, &db_pod_pb.MigrateDatabaseRequest{ReplicaAddress: replicaAddress})
	if err != nil {
		return err
	}

	// Shutdown primary
	err = c.stopDatabase(ctx, primaryDB, false)
	if err != nil {
		return err
	}

	primaryDB.SetPod(replicaPod)
	return nil
}

func (c *Controller) reapIdleDatabases(ctx context.Context) {
	for _, db := range c.databases.All() {
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

// Routes a database to it's pod address. Context should be timed
func (c *Controller) RouteDatabase(ctx context.Context, databaseID string) (string, error) {
	foundDB, exists := c.databases.Load(databaseID)
	if !exists {
		return "", core.ErrDatabaseNotFound
	}

	foundDB.Lock()
	defer foundDB.Unlock()

	if foundDB.IsDeleted() {
		return "", core.ErrDatabaseNotFound
	}

	if !foundDB.IsRunning() {
		if err := c.resumeDatabaseHelper(ctx, foundDB); err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("%s:%d", foundDB.Pod().IP(), c.proxyPort), nil
}

// Returns a short-lived presigned URL for uploading the database's snapshot.
// Only the pod currently assigned to the database (identified by callerIP) may request one.
func (c *Controller) SnapshotPutURL(ctx context.Context, databaseID string, callerIP string) (string, error) {
	db, exists := c.databases.Load(databaseID)
	if !exists || db.IsDeleted() {
		return "", core.ErrDatabaseNotFound
	}

	if !db.IsRunning() {
		return "", core.ErrDatabasePaused
	}

	if db.Pod().IP() != callerIP {
		return "", ErrCallerNotAssignedPod
	}

	return c.snapShotstorage.PresignedPutURL(ctx, databaseID, SNAPSHOT_UPLOAD_URL_EXPIRY)
}

func (c *Controller) RecordDatabaseActivity(databaseID string) error {
	database, exists := c.databases.Load(databaseID)
	if !exists {
		return core.ErrDatabaseNotFound
	}
	database.RecordActivity()
	return nil
}

func (c *Controller) StopAllWorkers() error {
	return nil // Implement me
}
