package leadergrpcserver

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/Technically56/HaToKuSe/internal/leader/core/hashring"
	pb "github.com/Technically56/HaToKuSe/proto/leaderservice"
)

type NodeGrpcServer struct {
	pb.UnimplementedLeaderServiceServer
	hr      *hashring.HashRing
	isAlive *sync.Map
}

func NewNodeGrpcServer() *NodeGrpcServer {
	hr := hashring.NewHashRing()
	isAlive := &sync.Map{}
	return &NodeGrpcServer{
		hr: hr, isAlive: isAlive,
	}
}

func (s *NodeGrpcServer) JoinFamily(ctx context.Context, in *pb.FamilyJoinRequest) (*pb.FamilyJoinResponse, error) {
	addr := in.GetIp() + ":" + in.GetPort()
	node_id := in.GetNodeId()
	err := s.hr.AddNode(node_id, addr)
	if err != nil {
		return &pb.FamilyJoinResponse{Success: false}, nil
	}
	return &pb.FamilyJoinResponse{Success: true}, nil
}
func (s *NodeGrpcServer) GetFamilyList(ctx context.Context, in *pb.FamilyListRequest) (*pb.FamilyListResponse, error) {
	members_id, members_addr := s.hr.GetCurrentMembers()
	memberResponses := make([]*pb.FamilyMember, 0, len(members_addr))
	for i, memberAddr := range members_addr {
		parts := strings.SplitN(memberAddr, ":", 2)
		memberResponses = append(memberResponses, &pb.FamilyMember{MemberId: members_id[i], Ip: parts[0], Port: parts[1]})
	}
	return &pb.FamilyListResponse{Members: memberResponses}, nil
}
func (s *NodeGrpcServer) SendHeartbeat(ctx context.Context, in *pb.HeartBeatRequest) (*pb.HeartBeatResponse, error) {
	node_id := in.GetMemberId()
	currentTime := time.Now().Unix()
	s.isAlive.Store(node_id, currentTime)
	return &pb.HeartBeatResponse{}, nil
}
