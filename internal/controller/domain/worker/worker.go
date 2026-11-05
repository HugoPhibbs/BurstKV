package worker

import (
	"burstkv/internal/workerpb"
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Worker struct {
	WorkerID      string
	ProxyAddress  string
	RPCAddress    string
	RPCConnection *grpc.ClientConn
	client        workerpb.WorkerClient
}

func NewWorker(workerID string, grpcAddress string, proxyAddress string) (*Worker, error) {
	conn, err := grpc.NewClient(grpcAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	
	if err != nil {
		return nil, err
	}

	return &Worker{
		WorkerID:     workerID,
		ProxyAddress: proxyAddress,
		RPCAddress:   grpcAddress,
		RPCConnection: conn,
		client:       workerpb.NewWorkerClient(conn),
	}, nil
}

func (w *Worker) DeleteDatabase(databaseID string) error {
	_, err := w.client.DeleteDatabase(context.Background(), &workerpb.DatabaseRequest{
		DatabaseId: databaseID,
	})
	return err
}

func (w *Worker) CreateDatabase(databaseID string) error {
	_, err := w.client.CreateDatabase(context.Background(), &workerpb.DatabaseRequest{
		DatabaseId: databaseID,
	})
	return err
}

func (w *Worker) ResumeDatabase(databaseID string) error {
	_, err := w.client.ResumeDatabase(context.Background(), &workerpb.DatabaseRequest{
		DatabaseId: databaseID,
	})
	return err
}

func (w *Worker) PauseDatabase(databaseID string) error {
	_, err := w.client.PauseDatabase(context.Background(), &workerpb.DatabaseRequest{
		DatabaseId: databaseID,
	})
	return err
}

func (w *Worker) Status() (*workerpb.StatusResponse, error) {
	resp, err := w.client.Status(context.Background(), &workerpb.Empty{})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (w *Worker) CanAssignDatabase() bool {
	status, err := w.Status()
	if err != nil {
		return false
	}
	return status.CanAssign
}

func (w *Worker) AssignedDatabases() ([]string, error) {
	status, err := w.Status()
	if err != nil {
		return nil, err
	}
	return status.AssignedDatabases, nil
}