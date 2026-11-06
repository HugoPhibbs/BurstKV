package grpc

import (
	"context"
	"errors"
	"net"
	"sync"

	"burstkv/internal/worker"
	pb "burstkv/internal/workerpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCServer struct {
	pb.UnimplementedWorkerServer
	mu sync.Mutex // WorkerManager is not safe for concurrent use
	worker *worker.Worker
}

func New(worker *worker.Worker) *GRPCServer {
	return &GRPCServer{worker: worker}
}

func (s *GRPCServer) ListenAndServe(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterWorkerServer(grpcServer, s)
	return grpcServer.Serve(lis)
}


func (s *GRPCServer) CreateDatabase(ctx context.Context, req *pb.DatabaseRequest) (*pb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := requestHasDatabaseID(req); err != nil {
		return nil, err
	}
	err := s.worker.CreateDatabase(req.GetDatabaseId())
	if err != nil {
		return nil,toStatus(err)
	}
	return &pb.Empty{}, nil
}

func (s *GRPCServer) ResumeDatabase(ctx context.Context, req *pb.DatabaseRequest) (*pb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := requestHasDatabaseID(req); err != nil {
		return nil, err
	}
	err := s.worker.ResumeDatabase(req.GetDatabaseId())
	if err != nil {
		return nil,toStatus(err)
	}
	return &pb.Empty{}, nil
}

func (s *GRPCServer) PauseDatabase(ctx context.Context, req *pb.DatabaseRequest) (*pb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := requestHasDatabaseID(req); err != nil {
		return nil, err
	}
	if err := s.worker.PauseDatabase(ctx, req.GetDatabaseId()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.Empty{}, nil
}

func (s *GRPCServer) DeleteDatabase(ctx context.Context, req *pb.DatabaseRequest) (*pb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := requestHasDatabaseID(req); err != nil {
		return nil, err
	}
	if err := s.worker.DeleteDatabase(ctx, req.GetDatabaseId()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.Empty{}, nil
}

func (s *GRPCServer) Status(ctx context.Context, _ *pb.Empty) (*pb.StatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &pb.StatusResponse{
		CanAssign:           s.worker.CanAssignDatabase(),
		AssignedDatabases:   s.worker.AssignedDatabases(),
		AvailableContainers: int32(s.worker.AvailableContainerCount()),
	}, nil
}

// Maps WorkerManager errors to gRPC status codes.
func toStatus(err error) error {
	switch {
	case errors.Is(err, worker.ErrNoAvailableContainers):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, worker.ErrDatabaseNotAssigned):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func requestHasDatabaseID(req *pb.DatabaseRequest) error {
	if req.GetDatabaseId() == "" {
		return status.Error(codes.InvalidArgument, "database_id is required")
	}
	return nil
}
