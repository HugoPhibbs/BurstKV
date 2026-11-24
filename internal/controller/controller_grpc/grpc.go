package controller_grpc

import (
	"burstkv/internal/controller"
	pb "burstkv/internal/controller/controller_pb"
	"burstkv/internal/core"
	"burstkv/internal/core/database_status"
	"burstkv/internal/core/pod_resources"
	"context"
	"errors"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
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

func (s *ControllerGRPCServer) GetDatabaseStatus(ctx context.Context, req *pb.GetDatabaseStatusRequest) (*pb.GetDatabaseStatusResponse, error) {
	status, err := s.controller.GetDatabaseStatus(ctx, req.GetDatabaseId())
	if err != nil {
		return nil, toRPCStatusError(err)
	}

	return &pb.GetDatabaseStatusResponse{
		Status: toPBDatabaseStatus(status),
	}, nil
}

func (s *ControllerGRPCServer) GetAllDatabaseStatus(ctx context.Context, _ *emptypb.Empty) (*pb.GetAllDatabaseStatusResponse, error) {
	statuses, err := s.controller.GetAllDatabaseStatuses(ctx)
	if err != nil {
		return nil, toRPCStatusError(err)
	}

	resp := &pb.GetAllDatabaseStatusResponse{
		Statuses: make(map[string]*pb.DatabaseStatus, len(statuses)),
	}
	for id, st := range statuses {
		resp.Statuses[id] = toPBDatabaseStatus(st)
	}
	return resp, nil
}

func toPBDatabaseStatus(dbs *database_status.DatabaseStatus) *pb.DatabaseStatus {
	if dbs == nil {
		return nil
	}
	return &pb.DatabaseStatus{
		Running:          dbs.Running,
		PodName:          dbs.PodName,
		MaxPodResources:  toPBPodResource(dbs.MaxPodResources),
		UsedPodResources: toPBPodResource(dbs.UsedPodResources),
	}
}

func toPBPodResource(r *pod_resources.PodResources) *pb.PodResource {
	if r == nil {
		return nil
	}
	return &pb.PodResource{Memory: r.Memory, Cpu: r.Cpu}
}

func (s *ControllerGRPCServer) GetSnapshotPutURL(ctx context.Context, req *pb.GetSnapshotUploadURLRequest) (*pb.GetSnapshotUploadURLResponse, error) {
	p, ok := peer.FromContext(ctx) // Check if the context of the peer is non-empty (always set for network calls)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "unknown caller")
	}
	callerIP, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "unknown caller")
	}

	url, err := s.controller.SnapshotPutURL(ctx, req.GetDatabaseId(), callerIP)
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

func (s *ControllerGRPCServer) RequestMigration(ctx context.Context, req *pb.RequestMigrationRequest) (*emptypb.Empty, error) {
	return nil, nil // TODO
}

func (s *ControllerGRPCServer) CreateDatabase(ctx context.Context, _ *emptypb.Empty) (*pb.CreateDatabaseResponse, error) {
	id, password, err := s.controller.CreateDatabase(ctx)
	if err != nil {
		return nil, toRPCStatusError(err)
	}
	return &pb.CreateDatabaseResponse{DatabaseId: id, Password: password}, nil
}

func (s *ControllerGRPCServer) PauseDatabase(ctx context.Context, req *pb.PauseDatabaseRequest) (*emptypb.Empty, error) {
	if err := s.controller.PauseDatabase(ctx, req.GetDatabaseId()); err != nil {
		return nil, toRPCStatusError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *ControllerGRPCServer) ResumeDatabase(ctx context.Context, req *pb.ResumeDatabaseRequest) (*emptypb.Empty, error) {
	if err := s.controller.ResumeDatabase(ctx, req.GetDatabaseId()); err != nil {
		return nil, toRPCStatusError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *ControllerGRPCServer) GetControllerConfig(ctx context.Context, _ *emptypb.Empty) (*pb.GetControllerConfigResponse, error) {
	config, err := s.controller.GetConfig(ctx)

	if err != nil {
		return nil, err
	}

	return &pb.GetControllerConfigResponse{
		ProxyPort:  int64(config.ProxyPort),
		StorageUrl: config.StorageURL,
		IdleLimit:  int64(config.IdleLimit),
	}, nil
}

func (s *ControllerGRPCServer) GetControllerStatus(ctx context.Context, _ *emptypb.Empty) (*pb.GetControllerStatusResponse, error) {
	status, err := s.controller.GetStatus(ctx)

	if err != nil {
		return nil, err
	}

	return &pb.GetControllerStatusResponse{
		NumDatabases:     int64(status.NumDatabases),
		NumPodsAvailable: int64(status.NumPodsAvailable),
	}, nil
}

func (s *ControllerGRPCServer) DeleteDatabase(ctx context.Context, req *pb.DeleteDatabaseRequest) (*emptypb.Empty, error) {
	if err := s.controller.DeleteDatabase(ctx, req.GetDatabaseId()); err != nil {
		return nil, toRPCStatusError(err)
	}
	return &emptypb.Empty{}, nil
}

// toRPCStatusError maps controller errors to gRPC status codes, so clients can
// tell "not found" apart from "something broke" without parsing the message.
func toRPCStatusError(err error) error {
	code := codes.Internal
	switch {
	case errors.Is(err, core.ErrDatabaseNotFound):
		code = codes.NotFound
	case errors.Is(err, core.ErrDatabaseAlreadyPaused),
		errors.Is(err, core.ErrDatabaseAlreadyActive):
		code = codes.FailedPrecondition
	case errors.Is(err, core.ErrNoAvailablePods):
		code = codes.Unavailable
	}
	return status.Error(code, err.Error())
}
