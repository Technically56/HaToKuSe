package nodeserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/Technically56/HaToKuSe/internal/config"
	nodegrpcserver "github.com/Technically56/HaToKuSe/internal/node/network/grpcServer"
	fm "github.com/Technically56/HaToKuSe/internal/node/storage/filemanager"
	pb_leader "github.com/Technically56/HaToKuSe/proto/leaderservice"
	pb_node "github.com/Technically56/HaToKuSe/proto/nodeservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeServer struct {
	grpc_handler  *nodegrpcserver.NodeGrpcServer
	leader_conn   *grpc.ClientConn
	leader_client pb_leader.LeaderServiceClient
	node_id       string
	config        *config.Config
	grpc_engine   *grpc.Server

	family_members []string
	mu             sync.RWMutex

	stopCtx    context.Context
	cancelStop context.CancelFunc
}

func NewNodeServer(cfg *config.Config, file_manager fm.FileManagerInterface, node_id string) (*NodeServer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	ns := &NodeServer{
		config:     cfg,
		stopCtx:    ctx,
		cancelStop: cancel,

		grpc_handler: nodegrpcserver.NewNodeGrpcServer(file_manager),
	}

	ns.node_id = node_id
	return ns, nil
}

func (ns *NodeServer) Start(leader_addr string, node_port string) error {
	port := node_port

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %v", port, err)
	}

	ns.grpc_engine = grpc.NewServer()
	pb_node.RegisterNodeServiceServer(ns.grpc_engine, ns.grpc_handler)

	go func() {
		log.Printf("Node [%s] serving gRPC on :%s", ns.node_id, port)
		if err := ns.grpc_engine.Serve(lis); err != nil {
			log.Printf("gRPC engine stopped: %v", err)
		}
	}()

	if err := ns.connectToHost(leader_addr); err != nil {
		return err
	}

	myIP := ns.getConfigValue("node", "ip")
	if myIP == "" {
		detected, err := ns.getDockerIP()
		if err != nil {
			hostname, _ := os.Hostname()
			myIP = hostname
		} else {
			myIP = detected
		}
	}

	joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = ns.leader_client.JoinFamily(joinCtx, &pb_leader.FamilyJoinRequest{
		Ip:     myIP,
		NodeId: ns.node_id,
		Port:   port,
	})
	if err != nil {
		return fmt.Errorf("failed to join family: %v", err)
	}

	go ns.startHeartBeatRoutine(5)
	go ns.startDiscoveryRoutine(10)

	log.Printf("Node started successfully. Identity: %s:%s", myIP, port)
	return nil
}

func (ns *NodeServer) Stop() error {
	log.Println("Initiating graceful shutdown...")

	ns.cancelStop()

	if ns.leader_conn != nil {
		ns.leader_conn.Close()
	}
	if ns.grpc_engine != nil {
		ns.grpc_engine.GracefulStop()
	}

	return nil
}

func (ns *NodeServer) startHeartBeatRoutine(interval_sec int) {
	ticker := time.NewTicker(time.Duration(interval_sec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := ns.leader_client.SendHeartbeat(ctx, &pb_leader.HeartBeatRequest{
				MemberId: ns.node_id,
			})
			if err != nil {
				log.Printf("Heartbeat failed: %v", err)
			}
			cancel()
		case <-ns.stopCtx.Done():
			log.Println("Stopping heartbeat routine...")
			return
		}
	}
}

func (ns *NodeServer) startDiscoveryRoutine(interval_sec int) {
	ticker := time.NewTicker(time.Duration(interval_sec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ns.updateFamilyList()
		case <-ns.stopCtx.Done():
			log.Println("Stopping discovery routine...")
			return
		}
	}
}

func (ns *NodeServer) updateFamilyList() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := ns.leader_client.GetFamilyList(ctx, &pb_leader.FamilyListRequest{})
	if err != nil {
		log.Printf("Discovery update failed: %v", err)
		return
	}

	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.family_members = nil
	for _, m := range resp.Members {
		ns.family_members = append(ns.family_members, m.MemberId)
	}
}

// Helpers
func (ns *NodeServer) connectToHost(addr string) error {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return fmt.Errorf("leader connection failed: %v", err)
	}

	ns.leader_conn = conn
	ns.leader_client = pb_leader.NewLeaderServiceClient(conn)
	return nil
}

func (ns *NodeServer) getDockerIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("no docker ip found")
}

func (ns *NodeServer) getConfigValue(section, key string) string {
	if s, ok := ns.config.Values[section]; ok {
		if val, ok := s[key]; ok {
			return fmt.Sprintf("%v", val)
		}
	}
	return ""
}

func (ns *NodeServer) GetCurrentMembers() []string {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	res := make([]string, len(ns.family_members))
	copy(res, ns.family_members)
	return res
}
