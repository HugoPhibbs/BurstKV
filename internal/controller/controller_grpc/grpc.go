package controller_grpc

import (
	"burstkv/internal/controller"
	"burstkv/internal/core"
	pb "burstkv/internal/controller/controller_pb"
	"context"
	"errors"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type ControllerGRPCServer struct {
	pb.UnimplementedControllerServer
	controller *controller.Controller
}

func NewControllerGRPCServer(c *controller.Controller) *ControllerGRPCServer {
	return &ControllerGRPCServer{controller: c}
}

func (s *ControllerGRPCServer) ListenAndServe(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterControllerServer(grpcServer, s)
	return grpcServer.Serve(lis)
}

func (s *ControllerGRPCServer) GetSnapshotUploadURL(ctx context.Context, req *pb.GetSnapshotUploadURLRequest) (*pb.GetSnapshotUploadURLResponse, error) {
	p, ok := peer.FromContext(ctx) // Check if the context of the peer is non-empty (always set for network calls)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "unknown caller")
	}
	callerIP, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "unknown caller")
	}

	url, err := s.controller.SnapshotUploadURL(ctx, req.GetDatabaseId(), callerIP)
	switch {
	case errors.Is(err, core.ErrDatabaseNotFound):
		return nil, status.Error(codes.NotFound, err.Error())
	case errors.Is(err, controller.ErrCallerNotAssignedPod):
		return nil, status.Error(codes.PermissionDenied, err.Error())
	case err != nil:
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.GetSnapshotUploadURLResponse{Url: url}, nil
}
