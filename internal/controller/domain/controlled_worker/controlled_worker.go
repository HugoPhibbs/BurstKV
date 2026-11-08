package controlled_worker

import (
	"burstkv/internal/workerpb"
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ControlledWorker struct {
	WorkerID      string
	ProxyAddress  string
	RPCAddress    string
	RPCConnection *grpc.ClientConn
	client        workerpb.WorkerClient
}

func NewControlledWorker(workerID string, grpcAddress string, proxyAddress string) (*ControlledWorker, error) {
	conn, err := grpc.NewClient(grpcAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, err
	}

	return &ControlledWorker{
		WorkerID:      workerID,
		ProxyAddress:  proxyAddress,
		RPCAddress:    grpcAddress,
		RPCConnection: conn,
		client:        workerpb.NewWorkerClient(conn),
	}, nil
}

func (w *ControlledWorker) DeleteDatabase(ctx context.Context ,databaseID string) error {
	_, err := w.client.DeleteDatabase(ctx, &workerpb.DatabaseRequest{
		DatabaseId: databaseID,
	})
	return err
}

func (w *ControlledWorker) CreateDatabase(ctx context.Context, databaseID string) error {
	_, err := w.client.CreateDatabase(ctx, &workerpb.DatabaseRequest{
		DatabaseId: databaseID,
	})
	return err
}

func (w *ControlledWorker) ResumeDatabase(ctx context.Context, databaseID string) error {
	_, err := w.client.ResumeDatabase(ctx, &workerpb.DatabaseRequest{
		DatabaseId: databaseID,
	})
	return err
}

func (w *ControlledWorker) PauseDatabase(ctx context.Context, databaseID string) error {
	_, err := w.client.PauseDatabase(ctx, &workerpb.DatabaseRequest{
		DatabaseId: databaseID,
	})
	return err
}

func (w *ControlledWorker) WaitUntilRPCIsReady(ctx context.Context) error {
	_, err := w.client.Status(ctx, &workerpb.Empty{}, grpc.WaitForReady(true))
	return err
}

func (w *ControlledWorker) Status(ctx context.Context) (*workerpb.StatusResponse, error) {
	resp, err := w.client.Status(ctx, &workerpb.Empty{})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (w *ControlledWorker) CanAssignDatabase(ctx context.Context) bool {
	status, err := w.Status(ctx)
	if err != nil {
		return false
	}
	return status.CanAssign
}

func (w *ControlledWorker) AssignedDatabases(ctx context.Context) ([]string, error) {
	status, err := w.Status(ctx)
	if err != nil {
		return nil, err
	}
	return status.AssignedDatabases, nil
}
