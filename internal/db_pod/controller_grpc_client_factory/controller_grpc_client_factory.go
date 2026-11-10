package controller_grpc_client_factory

import (
	"burstkv/internal/controller/controller_pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ControllerGRPCClientFactory struct {
	address string
}

func NewControllerGRPCClientFactory(address string) *ControllerGRPCClientFactory {
	return &ControllerGRPCClientFactory{address: address}
}

func (c *ControllerGRPCClientFactory) NewConnectedClient() (controller_pb.ControllerClient, func() error, error) {
	conn, err := grpc.NewClient(c.address,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return controller_pb.NewControllerClient(conn), conn.Close, nil
}
