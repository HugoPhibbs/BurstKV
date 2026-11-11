package db_pod

import (
	"burstkv/internal/controller/controller_pb"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const DEFAULT_DATA_DIR = "/data"

var ErrSnapshotNotFound = errors.New("snapshot not found")

type ControllerGRPCClientFactory interface {
	NewConnectedClient() (controller_pb.ControllerClient, func() error, error)
}

type DBPod struct {
	databaseID    string
	redis         *exec.Cmd
	redisPort     int
	dataDir       string
	controllerClientFactory ControllerGRPCClientFactory
}

func NewDBPod(redisPort int, controllerClientFactory ControllerGRPCClientFactory) *DBPod {
	return &DBPod{
		redisPort: redisPort,
		dataDir:   DEFAULT_DATA_DIR,
		controllerClientFactory: controllerClientFactory,
	}
}

func (d *DBPod) dumpPath() string {
	return filepath.Join(d.dataDir, "dump.rdb")
}

func (d *DBPod) Assign(ctx context.Context, databaseID string, getURL string, createNew bool) error {
	d.databaseID = databaseID

	if !createNew {
		if err := d.loadSnapshot(ctx, getURL); err != nil {
			return err
		}
	}

	// Not CommandContext: redis must outlive the request that started it.
	cmd := exec.Command("redis-server", "--dir", d.dataDir, "--port", strconv.Itoa(d.redisPort), "--requirepass", databaseID)
	if err := cmd.Start(); err != nil {
		return err
	}
	d.redis = cmd

	if err := d.waitForRedisReady(ctx); err != nil {
		cmd.Process.Kill()
		cmd.Wait()
		d.redis = nil
		return err
	}
	return nil
}

func (d *DBPod) waitForRedisReady(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		out, err := exec.CommandContext(ctx, "redis-cli", "-p", strconv.Itoa(d.redisPort), "-a", d.databaseID, "--no-auth-warning", "PING").Output()
		if err == nil && strings.TrimSpace(string(out)) == "PONG" {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("redis not ready: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (d *DBPod) saveSnapshot(ctx context.Context) error {
	if err := exec.CommandContext(ctx, "redis-cli", "-p", strconv.Itoa(d.redisPort), "-a", d.databaseID, "--no-auth-warning", "SAVE").Run(); err != nil {
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
	res, err := controllerClient.GetSnapshotUploadURL(ctx, &controller_pb.GetSnapshotUploadURLRequest{DatabaseId: d.databaseID})
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
	if d.redis == nil {
		return ErrNoDatabaseRunningOnPod
	}
	if save {
		if err := d.saveSnapshot(ctx); err != nil {
			return err
		}
	}

	if err := exec.CommandContext(ctx, "redis-cli", "-p", strconv.Itoa(d.redisPort), "-a", d.databaseID, "--no-auth-warning", "SHUTDOWN", "NOSAVE").Run(); err != nil {
		return err
	}

	err := d.redis.Wait()
	d.redis = nil
	return err
}
