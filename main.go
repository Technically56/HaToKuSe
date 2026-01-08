package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Technically56/HaToKuSe/internal/config"
	"github.com/Technically56/HaToKuSe/internal/node/core/nodeserver"
	"github.com/Technically56/HaToKuSe/internal/node/storage/filemanager"
)

func main() {
	mode := flag.String("mode", "", "Mode : leader veya node")
	flag.Parse()

	if *mode == "" {
		fmt.Println("HATA : Lutfen bir mod belirtin -mode:<mode>")
		os.Exit(1)
	}

	const configPath = "config.yaml"
	const storagePath = "./storage_files"

	switch *mode {
	case "node":

		cfg, err := config.LoadConfig(configPath) // config dosyasini okuyor argumanlari aliyore
		if err != nil {
			log.Fatalf("Config hatasi: %v", err)
		}

		leaderAddr := fmt.Sprintf("%v", cfg.Values["node"]["leader_addr"]) // configten aldigimiz degelerde lider adresini buluyor

		fm, _ := filemanager.NewFileManager(storagePath, 4096, 1000, false) //filemanager.go yu calsiiyor

		if err != nil {
			log.Fatalf("FileManager hatası: %v", err)
		}

		server, err := nodeserver.NewNodeServer(configPath, fm)
		if err != nil {
			log.Fatalf("NodeServer hatası: %v", err)
		}

		log.Printf("Leader hedefleniyor: %s", leaderAddr)
		if err := server.Start(leaderAddr); err != nil {
			log.Fatalf("Başlatma hatası: %v", err)
		}

		fmt.Println("Node aktif. Kapatmak için CTRL+C'ye basın.")

		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit

		log.Println("Kapatılıyor...")
		server.Stop()

	case "leader":

	}

}
