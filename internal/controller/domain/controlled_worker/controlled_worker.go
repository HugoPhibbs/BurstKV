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

func (w *ControlledWorker) DeleteDatabase(databaseID string) error {
	_, err := w.client.DeleteDatabase(context.Background(), &workerpb.DatabaseRequest{
		DatabaseId: databaseID,
	})
	return err
}

func (w *ControlledWorker) CreateDatabase(databaseID string) error {
	_, err := w.client.CreateDatabase(context.Background(), &workerpb.DatabaseRequest{
		DatabaseId: databaseID,
	})
	return err
}

func (w *ControlledWorker) ResumeDatabase(databaseID string) error {
	_, err := w.client.ResumeDatabase(context.Background(), &workerpb.DatabaseRequest{
		DatabaseId: databaseID,
	})
	return err
}

func (w *ControlledWorker) PauseDatabase(databaseID string) error {
	_, err := w.client.PauseDatabase(context.Background(), &workerpb.DatabaseRequest{
		DatabaseId: databaseID,
	})
	return err
}

func (w *ControlledWorker) Status() (*workerpb.StatusResponse, error) {
	resp, err := w.client.Status(context.Background(), &workerpb.Empty{})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (w *ControlledWorker) CanAssignDatabase() bool {
	status, err := w.Status()
	if err != nil {
		return false
	}
	return status.CanAssign
}

func (w *ControlledWorker) AssignedDatabases() ([]string, error) {
	status, err := w.Status()
	if err != nil {
		return nil, err
	}
	return status.AssignedDatabases, nil
}
