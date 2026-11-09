package db_pod_grpc

import (
	"context"
	"net"

	"burstkv/internal/db_pod"
	pb "burstkv/internal/db_pod/db_pod_pb"

	"google.golang.org/grpc"
)

type GRPCServer struct {
	pb.UnimplementedDBPodServer
	dbPod *db_pod.DBPod
}

func New(d *db_pod.DBPod) *GRPCServer {
	return &GRPCServer{dbPod: d}
}

func (s *GRPCServer) ListenAndServe(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterDBPodServer(grpcServer, s)
	return grpcServer.Serve(lis)
}

func (s *GRPCServer) CreateDatabase(ctx context.Context, req *pb.AssignDatabaseRequest) (*pb.Empty, error) {
	err := s.dbPod.Assign(ctx, req.GetDatabaseId(), req.GetGetUrl(), req.GetPutUrl(), true)
	if err != nil {
		return nil, err 
	}
	return &pb.Empty{}, nil
}

func (s *GRPCServer) ResumeDatabase(ctx context.Context, req *pb.AssignDatabaseRequest) (*pb.Empty, error) {
	err := s.dbPod.Assign(ctx, req.GetDatabaseId(), req.GetGetUrl(), req.GetPutUrl(), false)
	if err != nil {
		return nil, err 
	}
	return &pb.Empty{}, nil
}

func (s *GRPCServer) Stop(ctx context.Context, req *pb.StopRequest) (*pb.Empty, error) {
	if err := s.dbPod.Stop(ctx, req.GetSave()); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}
