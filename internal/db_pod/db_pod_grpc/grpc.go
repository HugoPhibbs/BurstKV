package db_pod_grpc

import (
	"context"
	"net"

	"burstkv/internal/db_pod"
	pb "burstkv/internal/db_pod/db_pod_pb"

	"google.golang.org/grpc"
)

type PodGRPCServer struct {
	pb.UnimplementedDBPodServer
	dbPod *db_pod.DBPod
}

func NewPodGRPCServer(d *db_pod.DBPod) *PodGRPCServer {
	return &PodGRPCServer{dbPod: d}
}

func (s *PodGRPCServer) ListenAndServe(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterDBPodServer(grpcServer, s)
	return grpcServer.Serve(lis)
}

func (s *PodGRPCServer) Assign(ctx context.Context, req *pb.AssignDatabaseRequest) (*pb.Empty, error) {
	err := s.dbPod.Assign(ctx, req.GetDatabaseId(), req.GetGetUrl(), req.GetCreate())
	if err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

func (s *PodGRPCServer) Stop(ctx context.Context, req *pb.StopRequest) (*pb.Empty, error) {
	if err := s.dbPod.Stop(ctx, req.GetSave()); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}
