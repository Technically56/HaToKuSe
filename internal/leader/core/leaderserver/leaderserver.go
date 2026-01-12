package leaderserver

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Technically56/HaToKuSe/internal/config"
	leadergrpcserver "github.com/Technically56/HaToKuSe/internal/leader/network"
	leadercommunication "github.com/Technically56/HaToKuSe/proto/leaderservice"
	nodecommunication "github.com/Technically56/HaToKuSe/proto/nodeservice"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type ClientMetadata struct {
	ID       string
	Conn     net.Conn
	JoinedAt time.Time
	Ip       string
	Mu       sync.Mutex
}
type LeaderServer struct {
	grpc_engine     *grpc.Server
	grpc_handler    *leadergrpcserver.LeaderGrpcServer
	client_listener net.Listener

	config         *config.Config
	tolerance      int
	call_timeout   time.Duration
	server_timeout time.Duration

	simple_mode bool

	heartbeatctx    context.Context
	heartbeatcancel context.CancelFunc
	mu              sync.RWMutex
	clients         map[string]*ClientMetadata
}

func NewLeaderServer(config *config.Config, tolerance int, timeout time.Duration, simple_mode bool) *LeaderServer {
	grpc_handler := leadergrpcserver.NewLeaderGrpcServer(100)
	grpc_engine := grpc.NewServer()

	ctx, cancel := context.WithCancel(context.Background())
	return &LeaderServer{
		config:          config,
		tolerance:       tolerance,
		call_timeout:    timeout,
		server_timeout:  timeout,
		simple_mode:     simple_mode,
		grpc_engine:     grpc_engine,
		grpc_handler:    grpc_handler,
		heartbeatctx:    ctx,
		heartbeatcancel: cancel,
		clients:         make(map[string]*ClientMetadata),
	}
}

func (ls *LeaderServer) Start() error {
	go func() {
		if err := ls.startHeatBeatCleaner(ls.heartbeatctx); err != nil {
			fmt.Printf("Heartbeat cleaner error: %v\n", err)
		}
	}()

	grpcErrChan := make(chan error, 1)
	go func() {
		if err := ls.startGrpcServer(); err != nil {
			grpcErrChan <- err
		}
	}()

	clientErrChan := make(chan error, 1)
	go func() {
		if err := ls.startClientServer(); err != nil {
			clientErrChan <- err
		}
	}()

	select {
	case err := <-grpcErrChan:
		return fmt.Errorf("fatal error in gRPC server: %w", err)
	case err := <-clientErrChan:
		return fmt.Errorf("fatal error in client server: %w", err)
	case <-time.After(500 * time.Millisecond):
		fmt.Println("Leader Server fully operational.")
	}

	return nil
}

func (ls *LeaderServer) Stop() error {
	fmt.Println("Shutting down Leader Server...")

	if ls.grpc_engine != nil {
		ls.grpc_engine.GracefulStop()
		fmt.Println("gRPC engine stopped.")
	}

	ls.mu.Lock()
	for id, client := range ls.clients {
		client.Mu.Lock()
		client.Conn.Close()
		client.Mu.Unlock()
		delete(ls.clients, id)
	}
	ls.mu.Unlock()
	fmt.Println("All client connections closed.")

	ls.client_listener.Close()
	fmt.Println("Client listener closed.")
	ls.heartbeatcancel()
	fmt.Println("Heartbeat cleaner stopped.")

	fmt.Println("Leader Server shutdown complete.")
	return nil
}

func (ls *LeaderServer) startClientServer() error {
	port := ls.config.Values["leader"]["client_port"].(string)
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	ls.client_listener = lis
	if err != nil {
		return fmt.Errorf("Failed to listen on port %s: %v", port, err)
	}
	fmt.Printf("Leader Client Server listening on %s\n", port)
	for {
		conn, err := ls.client_listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept client connection: %v\n", err)
			continue
		}
		go ls.simpleHandleClientConnection(conn)
	}
}
func (ls *LeaderServer) startGrpcServer() error {
	port := ls.config.Values["leader"]["grpc_port"].(string)
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		return fmt.Errorf("Failed to listen on port %s: %v", port, err)
	}
	leadercommunication.RegisterLeaderServiceServer(ls.grpc_engine, ls.grpc_handler)
	fmt.Printf("Leader gRPC Server listening on %s\n", port)
	if err := ls.grpc_engine.Serve(lis); err != nil {
		return fmt.Errorf("gRPC server stopped: %v", err)
	}
	return nil
}
func (ls *LeaderServer) storeFile(file_id string, data []byte, meta *ClientMetadata) error {
	if _, err := uuid.Parse(file_id); err != nil {
		return fmt.Errorf("invalid file_id: %s", file_id)
	}
	hr := ls.grpc_handler.Hr
	index, err := hr.FindContainingNodeIndex(file_id)
	if err != nil {
		return err
	}
	hash_bytes := sha256.Sum256(data)
	file_hash := hex.EncodeToString(hash_bytes[:])
	original_index := index
	replication_count := 0
	visited_nodes := make(map[string]bool)

	full_circle := false

	for replication_count < ls.tolerance && !full_circle {
		node_addr, err := hr.GetAddrFromIndex(index)
		if err != nil {
			return err
		}

		if !visited_nodes[node_addr] {
			conn, err := hr.GetOrCreateConnection(node_addr)
			if err != nil {
				ls.reportToClient(meta, "ERROR", fmt.Sprintf("Failed to connect to node %s: %v\n", node_addr, err))
			} else {
				ls.reportToClient(meta, "ALERT", fmt.Sprintf("Storing file %s on node %s...\n", file_id, node_addr))

				client := nodecommunication.NewNodeServiceClient(conn)
				storectx, cancel := context.WithTimeout(context.Background(), ls.call_timeout)

				hash, err := client.StoreFile(storectx, &nodecommunication.File{
					FileId:      file_id,
					FileContent: data,
				})
				cancel()

				if err != nil {
					ls.reportToClient(meta, "WARNING", fmt.Sprintf("Failed to store on node %s: %v\n", node_addr, err))
				} else {
					if hash.FileHash != file_hash {
						ls.reportToClient(meta, "WARNING", fmt.Sprintf("Hash mismatch after storing file on node %s", node_addr))
						index = hr.Walk(index)
						continue
					}
					replication_count++
					visited_nodes[node_addr] = true
					ls.reportToClient(meta, "ALERT", fmt.Sprintf("Successfully stored on %s\n", node_addr))
				}
			}
		}

		index = hr.Walk(index)
		if index == original_index {
			full_circle = true
		}
	}

	if replication_count < ls.tolerance {
		ls.reportToClient(meta, "ERROR", fmt.Sprintf("Failed to achieve desired replication for file %s. Only %d replicas created.\n", file_id, replication_count))
		if ls.simple_mode {
			meta.Mu.Lock()
			fmt.Fprintf(meta.Conn, "ERROR\n")
			meta.Mu.Unlock()
		}
	} else {
		if ls.simple_mode {
			meta.Mu.Lock()
			fmt.Fprintf(meta.Conn, "OK\n")
			meta.Mu.Unlock()
		}
		ls.reportToClient(meta, "SUCCESS", fmt.Sprintf("File %s stored successfully with %d replicas.\n", file_id, replication_count))
	}

	return nil
}
func (ls *LeaderServer) retrieveFile(file_id string, client_meta *ClientMetadata) ([]byte, error) {
	if _, err := uuid.Parse(file_id); err != nil {
		return nil, fmt.Errorf("invalid file_id: %s", file_id)
	}
	hr := ls.grpc_handler.Hr
	startIndex, err := hr.FindContainingNodeIndex(file_id)
	if err != nil {
		return nil, err
	}

	visited := make(map[string]bool)
	currentIndex := startIndex

	for {
		node_addr, err := hr.GetAddrFromIndex(currentIndex)
		if err != nil {
			return nil, err
		}

		if !visited[node_addr] {
			visited[node_addr] = true

			conn, err := hr.GetOrCreateConnection(node_addr)
			if err == nil {
				client := nodecommunication.NewNodeServiceClient(conn)

				ctx, cancel := context.WithTimeout(context.Background(), ls.call_timeout)

				resp, err := client.GetFile(ctx, &nodecommunication.FileRequest{FileId: file_id})
				cancel()

				if err == nil && resp != nil {
					ls.reportToClient(client_meta, "SUCCESS", fmt.Sprintf("File %s found on %s\n", file_id, node_addr))
					if ls.simple_mode {

						fmt.Fprintf(client_meta.Conn, "OK ")
					}
					return resp.FileContent, nil
				}

				ls.reportToClient(client_meta, "WARNING", fmt.Sprintf("File not found on node %s\n", node_addr))
			}
		}

		currentIndex = hr.Walk(currentIndex)

		if currentIndex == startIndex {
			break
		}
	}

	if ls.simple_mode {
		fmt.Fprintf(client_meta.Conn, "ERROR\n")
	}
	return nil, fmt.Errorf("File %s not found in the network", file_id)
}
func (ls *LeaderServer) startHeatBeatCleaner(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping heartbeat cleaner...")
			return nil
		case <-ticker.C:
			fmt.Println("Performing heartbeat cleanup...")
			ls.grpc_handler.IsAlive.Range(func(key, value any) bool {
				lastSeen := value.(time.Time)
				if time.Since(lastSeen) > ls.server_timeout {
					fmt.Printf("Detected dead node: %s. Removing...\n", key.(string))
					ls.broadCastToAllClients(fmt.Sprintf("ALERT: Node %s disconnected (heartbeat timeout).", key.(string)))
					ls.grpc_handler.IsAlive.Delete(key)
					ls.grpc_handler.Hr.RemoveNode(key.(string))
					fmt.Printf("Successfully removed node %s from hash ring.\n", key.(string))
				}
				return true
			})
		}
	}
}
func (ls *LeaderServer) simpleHandleClientConnection(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered from panic in client handler: %v\n", r)
		}
	}()
	defer conn.Close()
	client_id := uuid.New().String()
	client_meta := &ClientMetadata{
		ID:       client_id,
		Conn:     conn,
		JoinedAt: time.Now(),
		Ip:       conn.RemoteAddr().String(),
	}

	ls.mu.Lock()
	ls.clients[client_id] = client_meta
	ls.mu.Unlock()

	defer func() {
		ls.mu.Lock()
		delete(ls.clients, client_id)
		ls.mu.Unlock()
		fmt.Printf("Client %s disconnected.\n", client_id)
	}()

	ls.reportToClient(client_meta, "ALERT", "Welcome to HaToKuSe Leader Server (DETAILED MODE)\n")

	scanner := bufio.NewScanner(conn)
	const maxCapacity = 2 * 1024 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		command := strings.ToUpper(parts[0])

		switch command {
		case "SET":
			if len(parts) < 3 {
				ls.reportToClient(client_meta, "ERROR", "Usage: SET <key> <data>\n")
				continue
			}
			key := parts[1]
			data := []byte(parts[2])
			err := ls.storeFile(key, data, client_meta)
			if err != nil {
				ls.reportToClient(client_meta, "ERROR", err.Error())
				continue
			}

		case "GET":
			if len(parts) < 2 {
				ls.reportToClient(client_meta, "ERROR", "Usage: GET <key>\n")
				continue
			}
			key := parts[1]
			content, err := ls.retrieveFile(key, client_meta)
			if err != nil {
				ls.reportToClient(client_meta, "ERROR", err.Error())
				if ls.simple_mode {
					fmt.Fprintf(client_meta.Conn, "ERROR\n")
				}
				continue
			}
			if ls.simple_mode {
				fmt.Fprintf(client_meta.Conn, "%s\n", content)
			}
			if !ls.simple_mode {
				fmt.Fprintf(client_meta.Conn, "[DATA: %d]: %s\n", len(content), content)
			}

		case "FILES":
			files := ls.getFileCounts()
			for ip, count := range files {
				fmt.Fprintf(conn, "%s: %d\n", ip, count)
			}
			fmt.Fprintf(conn, "OK\n")

		case "NODES":
			ids, ips := ls.grpc_handler.Hr.GetCurrentMembers()
			for i := 0; i < len(ids); i++ {
				fmt.Fprintf(conn, "Node: %s -> %s\n", ids[i], ips[i])
			}
			fmt.Fprintf(conn, "OK\n")

		case "QUIT":
			conn.Close()
			return

		default:
			ls.reportToClient(client_meta, "ERROR", "Unknown command\n")
		}

	}
}
func (ls *LeaderServer) broadCastToAllClients(message string) {
	if !ls.simple_mode {
		ls.mu.RLock()
		defer ls.mu.RUnlock()
		for _, client := range ls.clients {
			go func(c *ClientMetadata) {
				c.Mu.Lock()
				defer c.Mu.Unlock()
				fmt.Fprintf(c.Conn, "[BROADCAST]: %s\n", message)
			}(client)
		}
	}
}
func (ls *LeaderServer) getNodeCount() int {
	return ls.grpc_handler.Hr.GetNodeCount()
}
func (ls *LeaderServer) reportToClient(meta *ClientMetadata, code string, report_msg string) {
	if !ls.simple_mode {

		code_map := make(map[string]string)
		code_map["SUCCESS"] = "[SUCCESS]: "
		code_map["ALERT"] = "[ALERT]: "
		code_map["WARNING"] = "[WARNING]: "
		code_map["ERROR"] = "[ERROR]: "
		meta.Mu.Lock()
		defer meta.Mu.Unlock()
		fmt.Fprint(meta.Conn, code_map[code], report_msg)
	}
}

func (ls *LeaderServer) frameData(data []byte) []byte {
	var framedData []byte
	dataLen := len(data)
	header := fmt.Sprintf("[DATA:%d]: ", dataLen)
	encoded_data := base64.StdEncoding.EncodeToString(data)
	footer := "[/DATA]\n"
	framedData = append(framedData, []byte(header)...)
	framedData = append(framedData, []byte(encoded_data)...)
	framedData = append(framedData, []byte(footer)...)
	return framedData
}
func (ls *LeaderServer) parseFramedData(framedData []byte) ([]byte, error) {
	dataStr := string(framedData)
	if !strings.HasPrefix(dataStr, "[DATA:") || !strings.Contains(dataStr, "]: ") || !strings.HasSuffix(dataStr, "[/DATA]\n") {
		return nil, fmt.Errorf("invalid framed data format")
	}

	headerEnd := strings.Index(dataStr, "]: ")
	if headerEnd == -1 {
		return nil, fmt.Errorf("invalid framed data format")
	}
	lengthStr := dataStr[6:headerEnd]
	var dataLength int
	_, err := fmt.Sscanf(lengthStr, "%d", &dataLength)
	if err != nil {
		return nil, fmt.Errorf("invalid data length in header")
	}

	dataStart := headerEnd + 3
	dataEnd := len(dataStr) - len("[/DATA]\n")
	encodedData := dataStr[dataStart:dataEnd]

	decodedData, err := base64.StdEncoding.DecodeString(encodedData)
	if err != nil {
		return nil, fmt.Errorf("base64 decoding failed: %v", err)
	}

	if len(decodedData) != dataLength {
		return nil, fmt.Errorf("data length mismatch: expected %d, got %d", dataLength, len(decodedData))
	}

	return decodedData, nil
}
func (ls *LeaderServer) getFileCounts() map[string]int {
	_, physicalIPs := ls.grpc_handler.Hr.GetCurrentMembers()
	fileCounts := make(map[string]int)

	for _, ip := range physicalIPs {
		conn, err := ls.grpc_handler.Hr.GetOrCreateConnection(ip)
		if err != nil {
			continue
		}
		client := nodecommunication.NewNodeServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), ls.call_timeout)
		resp, err := client.GetFileCount(ctx, &nodecommunication.FileCountRequest{})
		cancel()
		if err != nil {
			continue
		}
		fileCounts[ip] = int(resp.FileCount)
	}
	return fileCounts
}
