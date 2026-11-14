package db_pod

import (
	"burstkv/internal/controller/controller_pb"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const DEFAULT_DATA_DIR = "/data"
const DEFAULT_MIGRATION_TIMEOUT = 5000 // ms, since Redis uses these units (not native Go time duration)
const REDIS_START_TIMEOUT = 10 * time.Second

var ErrSnapshotNotFound = errors.New("snapshot not found")

type ControllerGRPCClientFactory interface {
	NewConnectedClient() (controller_pb.ControllerClient, func() error, error)
}

type DBPod struct {
	DatabaseID              string
	redisServerStartCmd     *exec.Cmd
	redisPort               int
	redisClient             *redis.Client
	dataDir                 string
	controllerClientFactory ControllerGRPCClientFactory
}

func NewDBPod(redisPort int, controllerClientFactory ControllerGRPCClientFactory) *DBPod {
	return &DBPod{
		redisPort:               redisPort,
		dataDir:                 DEFAULT_DATA_DIR,
		controllerClientFactory: controllerClientFactory,
	}
}

func (d *DBPod) dumpPath() string {
	return filepath.Join(d.dataDir, "dump.rdb")
}

func (d *DBPod) CreateDatabase(ctx context.Context, databaseID string) error {
	d.DatabaseID = databaseID

	return d.startRedisServer(ctx, databaseID, nil)
}

func (d *DBPod) ResumeDatabase(ctx context.Context, databaseID string, getURL string) error {
	d.DatabaseID = databaseID

	if err := d.loadSnapshot(ctx, getURL); err != nil {
		return err
	}

	return d.startRedisServer(ctx, databaseID, nil)
}

// Replicates the contents of another database into this pod
func (d *DBPod) ReplicateDatabase(ctx context.Context, replicaDatabaseID string, primaryAddress string) error {
	host, port, err := net.SplitHostPort(primaryAddress)
	if err != nil {
		return err
	}

	extraArgs := []string{"--replicaof", host, port, "--masterauth", replicaDatabaseID}

	if err := d.startRedisServer(ctx, replicaDatabaseID, extraArgs); err != nil {
		return err
	}

	err = pollUntil(ctx, 100*time.Millisecond, func(ctx context.Context) bool {
		info, err := d.replicationInfo(ctx)
		return err == nil && info["master_link_status"] == "up" && info["master_sync_in_progress"] == "0"
	})

	if err != nil {
		return err
	}

	d.DatabaseID = replicaDatabaseID

	return nil
}

func (d *DBPod) newRedisClient(password string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:" + strconv.Itoa(d.redisPort),
		Password: password,
	})
}

func (d *DBPod) startRedisServer(ctx context.Context, password string, extraArgs []string) error {
	args := []string{
		"--dir", d.dataDir,
		"--port", strconv.Itoa(d.redisPort),
		"--requirepass", password,
		"--masterauth", password, // for during a failover, this becomes the slave
		// Remove sync delay, see https://redis.io/docs/latest/operate/oss_and_stack/management/replication/#configuration
		"--repl-diskless-sync-delay", "0",
	}
	args = append(args, extraArgs...)
	cmd := exec.Command("redis-server", args...)
	if err := cmd.Start(); err != nil {
		return err
	}
	d.redisServerStartCmd = cmd

	readyCtx, cancel := context.WithTimeout(ctx, REDIS_START_TIMEOUT)
	defer cancel()

	d.redisClient = d.newRedisClient(password)

	err := pollUntil(readyCtx, 100*time.Millisecond, func(ctx context.Context) bool {
		return d.redisClient.Ping(ctx).Err() == nil
	})

	if err != nil {
		cmd.Process.Kill()
		cmd.Wait()
		d.redisClient.Close()
		d.redisServerStartCmd = nil
		return fmt.Errorf("Redis failed to start within %s, err: %w", REDIS_START_TIMEOUT, err)
	}

	return nil
}

// Migrate this Redis DB to a new address
func (d *DBPod) MigrateDatabase(ctx context.Context, replicaAddress string) error {
	if d.redisServerStartCmd == nil {
		return ErrNoDatabaseRunningOnPod
	}

	host, port, err := net.SplitHostPort(replicaAddress)
	if err != nil {
		return err
	}

	// Send the FAILOVER request
	reply, err := d.redisClient.Do(ctx, "FAILOVER", "TO", host, port, "TIMEOUT", strconv.Itoa(DEFAULT_MIGRATION_TIMEOUT)).Text()
	if reply != "OK" || err != nil {
		return err
	}

	// Wait for the FAILOVER to be completed
	var info map[string]string
	err = pollUntil(ctx, 50*time.Millisecond, func(ctx context.Context) bool {
		var err error
		info, err = d.replicationInfo(ctx)
		return err == nil && info["master_failover_state"] == "no-failover"
	})
	if err != nil {
		return fmt.Errorf("failover not complete: %w", err)
	}
	if info["role"] != "slave" { // Should be switched to slave (the replica should now be primary)
		return errors.New("failover aborted: still primary") // timed out or replica dropped
	}
	return nil
}

func (d *DBPod) replicationInfo(ctx context.Context) (map[string]string, error) {
	raw, err := d.redisClient.Info(ctx, "replication").Result()
	if err != nil {
		return nil, err
	}

	info := make(map[string]string)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line) // strips the \r
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if key, value, ok := strings.Cut(line, ":"); ok {
			info[key] = value
		}
	}

	return info, nil
}

func pollUntil(ctx context.Context, interval time.Duration, check func(ctx context.Context) bool) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if check(ctx) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (d *DBPod) saveSnapshot(ctx context.Context) error {
	if err := d.redisClient.Save(ctx).Err(); err != nil {
		return err
	}

	f, err := os.Open(d.dumpPath())
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}

	controllerClient, close, err := d.controllerClientFactory.NewConnectedClient()
	if err != nil {
		return err
	}
	defer close()
	res, err := controllerClient.GetSnapshotPutURL(ctx, &controller_pb.GetSnapshotUploadURLRequest{DatabaseId: d.DatabaseID})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, res.GetUrl(), f)
	if err != nil {
		return err
	}
	req.ContentLength = info.Size()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upload snapshot: %s", resp.Status)
	}
	return nil
}

func (d *DBPod) loadSnapshot(ctx context.Context, getURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, getURL, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return ErrSnapshotNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download snapshot: %s", resp.Status)
	}

	f, err := os.Create(d.dumpPath())
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(d.dumpPath())
		return err
	}
	return f.Close()
}

func (d *DBPod) Stop(ctx context.Context, save bool) error {
	if d.redisServerStartCmd == nil { // If redisCmd is nil, then nothing is currently running
		return ErrNoDatabaseRunningOnPod
	}
	if save {
		if err := d.saveSnapshot(ctx); err != nil {
			return err
		}
	}

	// By default, a client tries to reconnect to the server, even after it's shutdown (?)
	// So create a new client that doesn't try to reconnect.
	shutdownClient := d.newRedisClient(d.DatabaseID)
	shutdownClient.Options().MaxRetries = 0

	if err := shutdownClient.ShutdownNoSave(ctx).Err(); err != nil {
		return err
	}

	err := d.redisServerStartCmd.Wait()
	d.redisServerStartCmd = nil
	d.redisClient.Close()
	shutdownClient.Close()
	d.redisClient = nil
	return err
}

func (d *DBPod) HandleTermination(ctx context.Context, save bool) error {
	if d.redisServerStartCmd == nil { // No running Redis (e.g. Paused/Deleted/Unclaimed) so can do nothing
		return nil
	}
	client, closeConn, err := d.controllerClientFactory.NewConnectedClient()
	if err != nil { // Backup: controller is unresponsive, so just save a snapshot
		return d.saveSnapshot(ctx)
	}
	defer closeConn()
	_, err = client.RequestMigration(ctx, &controller_pb.RequestMigrationRequest{DatabaseId: d.DatabaseID})
	return err
}
