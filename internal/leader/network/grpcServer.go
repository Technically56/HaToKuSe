package leadergrpcserver

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/Technically56/HaToKuSe/internal/leader/core/hashring"
	pb "github.com/Technically56/HaToKuSe/proto/leaderservice"
)

type LeaderGrpcServer struct {
	pb.UnimplementedLeaderServiceServer
	Hr      *hashring.HashRing
	IsAlive *sync.Map
}

func NewLeaderGrpcServer(vnodeCount int) *LeaderGrpcServer {
	hr := hashring.NewHashRing(vnodeCount)
	isAlive := &sync.Map{}
	return &LeaderGrpcServer{
		Hr: hr, IsAlive: isAlive,
	}
}

func (s *LeaderGrpcServer) JoinFamily(ctx context.Context, in *pb.FamilyJoinRequest) (*pb.FamilyJoinResponse, error) {
	addr := in.GetIp() + ":" + in.GetPort()
	node_id := in.GetNodeId()
	err := s.Hr.AddNode(node_id, addr)
	if err != nil {
		return &pb.FamilyJoinResponse{Success: false}, nil
	}
	return &pb.FamilyJoinResponse{Success: true}, nil
}
func (s *LeaderGrpcServer) GetFamilyList(ctx context.Context, in *pb.FamilyListRequest) (*pb.FamilyListResponse, error) {
	members_id, members_addr := s.Hr.GetCurrentMembers()
	memberResponses := make([]*pb.FamilyMember, 0, len(members_addr))
	for i, memberAddr := range members_addr {
		parts := strings.SplitN(memberAddr, ":", 2)
		memberResponses = append(memberResponses, &pb.FamilyMember{MemberId: members_id[i], Ip: parts[0], Port: parts[1]})
	}
	return &pb.FamilyListResponse{Members: memberResponses}, nil
}
func (s *LeaderGrpcServer) SendHeartbeat(ctx context.Context, in *pb.HeartBeatRequest) (*pb.HeartBeatResponse, error) {
	node_id := in.GetMemberId()
	currentTime := time.Now().Unix()
	s.IsAlive.Store(node_id, currentTime)
	return &pb.HeartBeatResponse{}, nil
}
