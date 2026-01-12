package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Technically56/HaToKuSe/internal/config"
	"github.com/Technically56/HaToKuSe/internal/leader/core/leaderserver"
	"github.com/Technically56/HaToKuSe/internal/node/core/nodeserver"
	"github.com/Technically56/HaToKuSe/internal/node/storage/filemanager"
	"github.com/google/uuid"
)

func main() {
	mode := flag.String("mode", "", "Mode : leader or node")
	simple := flag.Bool("simple", false, "Enable simple mode (leader mode only)")
	port := flag.String("port", "", "Port to listen on (node mode only)")
	nodeID := flag.String("id", "", "Node ID (node mode only)")
	flag.Parse()

	if *mode == "" {
		fmt.Println("ERROR : Please specify a mode -mode:<mode>")
		os.Exit(1)
	}

	const configPath = "config.yaml"
	const storagePath = "./storage_files"

	switch *mode {
	case "node":

		cfg, err := config.LoadConfig(configPath)
		if err != nil {
			log.Fatalf("Config error: %v", err)
		}

		leaderAddr := fmt.Sprintf("%v", cfg.Values["node"]["leader_addr"])

		fm, err := filemanager.NewFileManager(storagePath, 4096, 1000, false)

		if err != nil {
			log.Fatalf("FileManager error: %v", err)
		}
		if *port != "" {
			cfg.Values["node"]["port"] = *port
		}

		var finalNodeID string
		if *nodeID != "" {
			finalNodeID = *nodeID
		} else {
			server_id, _ := uuid.NewRandom()
			finalNodeID = server_id.String()
		}

		server, err := nodeserver.NewNodeServer(cfg, fm, finalNodeID)
		if err != nil {
			log.Fatalf("NodeServer error: %v", err)
		}

		nodePort := fmt.Sprintf("%v", cfg.Values["node"]["port"])
		log.Printf("Targeting Leader: %s", leaderAddr)
		if err := server.Start(leaderAddr, nodePort); err != nil {
			log.Fatalf("Start error: %v", err)
		}

		fmt.Println("Node active. Press CTRL+C to exit.")

		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit

		log.Println("Shutting down...")
		server.Stop()

	case "leader":
		cfg, err := config.LoadConfig(configPath)
		if err != nil {
			log.Fatalf("Config error: %v", err)
		}

		fmt.Println("Starting Leader Server...")
		if *simple {
			fmt.Println("Simple Mode Enabled")
		}

		tolerance := 3
		if content, err := os.ReadFile("tolerance.conf"); err == nil {
			var t int
			if _, err := fmt.Sscanf(string(content), "%d", &t); err == nil && t > 0 {
				tolerance = t
				fmt.Printf("Loaded tolerance from config: %d\n", tolerance)
			} else {
				fmt.Println("Invalid format in tolerance.conf, using default: 3")
			}
		} else {
			fmt.Println("tolerance.conf not found, using default: 3")
		}

		ls := leaderserver.NewLeaderServer(cfg, tolerance, 5*time.Second, *simple)

		if err := ls.Start(); err != nil {
			log.Fatalf("Leader Server failed to start: %v", err)
		}

		fmt.Println("Leader Active!")
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit

		log.Println("Shutting down...")
		ls.Stop()

	}

}
