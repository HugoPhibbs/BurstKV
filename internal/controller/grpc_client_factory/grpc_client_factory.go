package grpc_client_factory

import (
	"burstkv/internal/db_pod/db_pod_pb"
	"burstkv/internal/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCClientFactory struct {
	port int
}

func NewGRPCClientFactory(port int) *GRPCClientFactory {
	return &GRPCClientFactory{port: port}
}

func (c *GRPCClientFactory) NewConnectedClient(databasePodIP string) (db_pod_pb.DBPodClient, func() error, error) {
	conn, err := grpc.NewClient(utils.FormatAddress(databasePodIP, c.port),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return db_pod_pb.NewDBPodClient(conn), conn.Close, nil
}
