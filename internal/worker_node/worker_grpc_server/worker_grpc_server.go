package worker_grpc_server

import (
	"context"
	"errors"
	"net"
	"sync"

	"burstkv/internal/utils"
	"burstkv/internal/worker_node"
	"burstkv/internal/worker_node/worker_manager"
	pb "burstkv/internal/workerpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type WorkerGRPCServer struct {
	pb.UnimplementedWorkerServer
	mu sync.Mutex // WorkerManager is not safe for concurrent use
	wm *worker_manager.WorkerManager
}

func New(wm *worker_manager.WorkerManager) *WorkerGRPCServer {
	return &WorkerGRPCServer{wm: wm}
}

func (s *WorkerGRPCServer) ListenAndServe(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterWorkerServer(grpcServer, s)
	return grpcServer.Serve(lis)
}

func (s *WorkerGRPCServer) AssignDatabase(ctx context.Context, req *pb.DatabaseRequest) (*pb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := requiresDatabaseID(req); err != nil {
		return nil, err
	}
	_, err := s.wm.AssignDatabase(req.GetDatabaseId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.Empty{}, nil
}

func (s *WorkerGRPCServer) PauseDatabase(ctx context.Context, req *pb.DatabaseRequest) (*pb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := requiresDatabaseID(req); err != nil {
		return nil, err
	}
	if err := s.wm.PauseDatabase(req.GetDatabaseId()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.Empty{}, nil
}

func (s *WorkerGRPCServer) DeleteDatabase(ctx context.Context, req *pb.DatabaseRequest) (*pb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := requiresDatabaseID(req); err != nil {
		return nil, err
	}
	if err := s.wm.DeleteDatabase(req.GetDatabaseId()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.Empty{}, nil
}

func (s *WorkerGRPCServer) DatabaseState(ctx context.Context, req *pb.DatabaseRequest) (*pb.DatabaseStateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := requiresDatabaseID(req); err != nil {
		return nil, err
	}
	state, err := s.wm.DatabaseState(req.GetDatabaseId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.DatabaseStateResponse{State: toPBState(state)}, nil
}

func (s *WorkerGRPCServer) Status(ctx context.Context, _ *pb.Empty) (*pb.StatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &pb.StatusResponse{
		CanAssign:         s.wm.CanAssignDatabase(),
		AssignedDatabases: s.wm.AssignedDatabases(),
		AvailableContainers: int32(s.wm.AvailableContainerCount()),
	}, nil
}

// Maps WorkerManager errors to gRPC status codes.
func toStatus(err error) error {
	switch {
	case errors.Is(err, worker_node.ErrNoAvailableContainers):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, worker_node.ErrDatabaseNotAssigned):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func toPBState(s utils.DBState) pb.DBState {
	switch s {
	case utils.Active:
		return pb.DBState_DB_STATE_ACTIVE
	case utils.Paused:
		return pb.DBState_DB_STATE_PAUSED
	case utils.NotFound:
		return pb.DBState_DB_STATE_NOT_FOUND
	default:
		return pb.DBState_DB_STATE_UNKNOWN
	}
}

func requiresDatabaseID(req *pb.DatabaseRequest) error {
	if req.GetDatabaseId() == "" {
		return status.Error(codes.InvalidArgument, "database_id is required")
	}
	return nil
}