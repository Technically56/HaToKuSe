package leadergrpcserver

import (
	"context"

	"github.com/Technically56/HaToKuSe/internal/leader/core/hashring"
	pb "github.com/Technically56/HaToKuSe/proto/leaderservice"
	"google.golang.org/grpc"
)

type NodeGrpcServer struct {
	pb.UnimplementedLeaderServiceServer
	hr *hashring.HashRing
}

func (s *NodeGrpcServer) JoinFamily(ctx context.Context, in *pb.FamilyJoinRequest, opts ...grpc.CallOption) (*pb.FamilyJoinResponse, error) {

}
func (s *NodeGrpcServer) GetFamilyList(ctx context.Context, in *pb.FamilyListRequest, opts ...grpc.CallOption) (*pb.FamilyListResponse, error) {

}
func (s *NodeGrpcServer) SendHeartbeat(ctx context.Context, in *pb.HeartBeatRequest, opts ...grpc.CallOption) (*pb.HeartBeatResponse, error) {

}
