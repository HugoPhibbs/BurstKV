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
	"strconv"
	"strings"
	"time"
)

const RDB_DUMP_PATH = "/data/dump.rdb"

var ErrSnapshotNotFound = errors.New("snapshot not found")

type ControllerGRPCClientFactory interface {
	NewConnectedClient() (controller_pb.ControllerClient, func() error, error)
}

type DBPod struct {
	databaseID    string
	redis         *exec.Cmd
	redisPort     int
	controllerClientFactory ControllerGRPCClientFactory
}

func NewDBPod(redisPort int, controllerClientFactory ControllerGRPCClientFactory) *DBPod {
	return &DBPod{
		redisPort: redisPort,
		controllerClientFactory: controllerClientFactory,
	}
}

func (d *DBPod) Assign(ctx context.Context, databaseID string, getURL string, createNew bool) error {
	d.databaseID = databaseID

	if !createNew {
		if err := d.loadSnapshot(ctx, getURL); err != nil {
			return err
		}
	}

	// Not CommandContext: redis must outlive the request that started it.
	cmd := exec.Command("redis-server", "--dir", "/data", "--port", strconv.Itoa(d.redisPort), "--requirepass", databaseID)
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
		out, err := exec.CommandContext(ctx, "redis-cli", "-a", d.databaseID, "--no-auth-warning", "PING").Output()
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
	if err := exec.CommandContext(ctx, "redis-cli", "-a", d.databaseID, "--no-auth-warning", "SAVE").Run(); err != nil {
		return err
	}

	f, err := os.Open(RDB_DUMP_PATH)
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

	f, err := os.Create(RDB_DUMP_PATH)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(RDB_DUMP_PATH)
		return err
	}
	return f.Close()
}

func (d *DBPod) Stop(ctx context.Context, save bool) error {
	if d.redis == nil {
		return nil
	}
	if save {
		if err := d.saveSnapshot(ctx); err != nil {
			return err
		}
	}

	if err := exec.CommandContext(ctx, "redis-cli", "-a", d.databaseID, "--no-auth-warning", "SHUTDOWN", "NOSAVE").Run(); err != nil {
		return err
	}

	err := d.redis.Wait()
	d.redis = nil
	return err
}
