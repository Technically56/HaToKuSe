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
)

func main() {
	mode := flag.String("mode", "", "Mode: leader or node")
	port := flag.String("port", "", "Port to listen on (node mode only)")
	storagePath := flag.String("storage_path", "./storage_files", "Path to store files (node mode only)")
	nodeID := flag.String("id", "", "Node ID (node mode only)")
	simple := flag.Bool("simple", false, "Enable simple mode (leader mode only)")

	flag.Parse()

	if *mode == "" {
		fmt.Println("Usage: go run test.go -mode=<leader|node> [-port=<port>] [-storage_path=<path>] [-id=<node_id>] [-simple]")
		os.Exit(1)
	}

	const configPath = "../config.yaml"

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	switch *mode {
	case "leader":
		fmt.Println("Starting Leader Server...")
		if *simple {
			fmt.Println("Simple Mode Enabled")
		}
		// Tolerance: 3, Timeout: 5s
		ls := leaderserver.NewLeaderServer(cfg, 3, 5*time.Second, *simple)

		if err := ls.Start(); err != nil {
			log.Fatalf("Leader Server failed to start: %v", err)
		}

		// Block until signal
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit

		log.Println("Shutting down Leader...")
		ls.Stop()

	case "node":
		fmt.Println("Starting Node Server...")

		// Override config values if flags are provided
		if *port != "" {
			cfg.Values["node"]["port"] = *port
		}
		if *nodeID != "" {
			cfg.Values["node"]["id"] = *nodeID
		} else if *port != "" {
			// If no ID provided but port is custom, append port to ID to make it unique-ish
			cfg.Values["node"]["id"] = fmt.Sprintf("%v-%v", cfg.Values["node"]["id"], *port)
		}

		leaderAddr := fmt.Sprintf("%v", cfg.Values["node"]["leader_addr"])

		// Initialize FileManager
		// Check if directory exists, create if not? NewFileManager might error if base path doesn't exist?
		// Actually NewFileManager checks: if _, err := os.Stat(folder_path); os.IsNotExist(err) { return nil, ... }
		// So we must ensure it exists.
		if err := os.MkdirAll(*storagePath, 0755); err != nil {
			log.Fatalf("Failed to create storage directory: %v", err)
		}

		fm, err := filemanager.NewFileManager(*storagePath, 4096, 1000, false)
		if err != nil {
			log.Fatalf("FileManager initialization failed: %v", err)
		}

		// Initialize NodeServer
		ns, err := nodeserver.NewNodeServer(cfg, fm, fmt.Sprintf("%v", cfg.Values["node"]["id"]))
		if err != nil {
			log.Fatalf("NodeServer initialization failed: %v", err)
		}

		listPort := cfg.Values["node"]["port"]
		log.Printf("Node %s targeting Leader at: %s using port %s", cfg.Values["node"]["id"], leaderAddr, listPort)
		if err := ns.Start(leaderAddr, fmt.Sprintf("%v", listPort)); err != nil {
			log.Fatalf("Node Server failed to start: %v", err)
		}

		fmt.Println("Node is active. Press CTRL+C to stop.")

		// Block until signal
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit

		log.Println("Shutting down Node...")
		ns.Stop()

	default:
		fmt.Printf("Unknown mode: %s. Use 'leader' or 'node'.\n", *mode)
		os.Exit(1)
	}
}
