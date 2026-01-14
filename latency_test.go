package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

var packetSize = flag.Int("packet_size", 1024, "Payload size in bytes")

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func generateRandomPayload(size int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func TestLatency(t *testing.T) {
	// Connect with timeout
	conn, err := net.DialTimeout("tcp", "127.0.0.1:6000", 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to Leader Server: %v", err)
	}
	defer conn.Close()

	// Set a deadline for all reads to prevent infinite hangs
	conn.SetDeadline(time.Now().Add(60 * time.Second))

	reader := bufio.NewReader(conn)

	// --- 1. Read Welcome Message ---
	// Expect: [ALERT]: Welcome to HaToKuSe Leader Server (DETAILED MODE)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read welcome message: %v", err)
	}
	t.Logf("Server Greeting: %s", strings.TrimSpace(line))

	id := uuid.New().String()
	payload := generateRandomPayload(*packetSize)
	t.Logf("Generated payload of size: %d bytes", len(payload))

	// Helper to read response lines until [SUCCESS] or [ERROR]
	readResponse := func(opName string) (string, bool) {
		var fullLog string
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("Failed to read response for %s: %v. Log so far:\n%s", opName, err, fullLog)
			}
			fullLog += line

			// Trim whitespace and prompts like "> "
			cleanLine := strings.TrimSpace(line)
			cleanLine = strings.TrimPrefix(cleanLine, "> ")

			if strings.HasPrefix(cleanLine, "[SUCCESS]") {
				return fullLog, true
			}
			if strings.HasPrefix(cleanLine, "[ERROR]") {
				return fullLog, false
			}
		}
	}

	// --- 2. SET Operation ---
	t.Logf("Testing SET with ID: %s", id)
	start := time.Now()

	_, err = fmt.Fprintf(conn, "SET %s %s\n", id, payload)
	if err != nil {
		t.Fatalf("Failed to send SET command: %v", err)
	}

	setLog, success := readResponse("SET")
	setDuration := time.Since(start)

	if !success {
		t.Fatalf("SET operation failed.\nServer Log:\n%s", setLog)
	}
	t.Logf("SET Latency: %v", setDuration)
	t.Logf("SET Response:\n%s", strings.TrimSpace(setLog))

	// --- 3. GET Operation ---
	t.Logf("Testing GET with ID: %s", id)
	start = time.Now()

	_, err = fmt.Fprintf(conn, "GET %s\n", id)
	if err != nil {
		t.Fatalf("Failed to send GET command: %v", err)
	}

	getLog, success := readResponse("GET")
	if !success {
		t.Fatalf("GET operation failed.\nServer Log:\n%s", getLog)
	}
	t.Logf("GET Metadata Response:\n%s", strings.TrimSpace(getLog))

	// After [SUCCESS], the server sends [DATA: len]: content
	dataLine, err := reader.ReadString('\n')
	getDuration := time.Since(start) // Measure latency including data receipt

	if err != nil {
		t.Fatalf("Failed to read data line after success: %v", err)
	}
	t.Logf("GET Data Line received.")

	// Parse [DATA: len]: content
	// Format matches: fmt.Fprintf(client_meta.Conn, "[DATA: %d]: %s\n", len(content), content)
	// Note the space after DATA:

	dataLine = strings.TrimSpace(dataLine)
	if !strings.HasPrefix(dataLine, "[DATA:") {
		t.Fatalf("Unexpected data line format: %s", dataLine)
	}

	// Simple validation: check if content is present
	// We can try to extract the content part
	// Find the first occurrence of "]: " which separates header from content
	splitIdx := strings.Index(dataLine, "]: ")
	if splitIdx == -1 {
		t.Fatalf("Malformed data header (missing ']: '): %s", dataLine)
	}

	receivedContent := dataLine[splitIdx+3:]

	if receivedContent != payload {
		// Truncate for log if too long
		dispRecv := receivedContent
		if len(dispRecv) > 50 {
			dispRecv = dispRecv[:50] + "..."
		}
		dispExp := payload
		if len(dispExp) > 50 {
			dispExp = dispExp[:50] + "..."
		}

		t.Errorf("Content mismatch!\nExpected: %s\nReceived: %s", dispExp, dispRecv)
	} else {
		t.Logf("Content verification SUCCESS. Length: %d", len(receivedContent))
	}

	t.Logf("GET Latency: %v", getDuration)
}

func TestSimpleModeLatency(t *testing.T) {
	// 1. Connect to the server
	conn, err := net.DialTimeout("tcp", "127.0.0.1:6000", 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to Leader Server: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(60 * time.Second))
	reader := bufio.NewReader(conn)

	// In Simple Mode, there is NO welcome message.
	// We proceed directly to operations.

	id := uuid.New().String()
	// Reuse generateRandomPayload from latency_test.go (same package)
	// If packetSize is not init, it defaults to 1024.
	// Since both are in package main and TestMain parses flags, this uses the flag value.
	payload := generateRandomPayload(*packetSize)
	t.Logf("Generated payload of size: %d bytes", len(payload))

	// --- 2. SET Operation ---
	t.Logf("Testing SET (Simple Mode) with ID: %s", id)
	start := time.Now()

	_, err = fmt.Fprintf(conn, "SET %s %s\n", id, payload)
	if err != nil {
		t.Fatalf("Failed to send SET command: %v", err)
	}

	// Expect: "OK\n" or "ERROR\n"
	response, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read SET response: %v", err)
	}
	setDuration := time.Since(start)

	response = strings.TrimSpace(response)
	if response != "OK" {
		t.Fatalf("SET operation failed. Expected 'OK', got '%s'", response)
	}
	t.Logf("SET Latency: %v", setDuration)

	// --- 3. GET Operation ---
	t.Logf("Testing GET (Simple Mode) with ID: %s", id)
	start = time.Now()

	_, err = fmt.Fprintf(conn, "GET %s\n", id)
	if err != nil {
		t.Fatalf("Failed to send GET command: %v", err)
	}

	// Expect: "OK <content>\n"
	// Note: The code sends "OK " then "content\n".
	// So we might technically get it in one ReadString notification if small enough,
	// or we might get "OK " then the rest.
	// ReadString('\n') should capture until the end of the line (content end).

	getResponse, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read GET response: %v", err)
	}
	getDuration := time.Since(start)

	// Check prefix
	if !strings.HasPrefix(getResponse, "OK ") {
		t.Fatalf("GET operation failed or invalid format. Received: %s", getResponse)
	}

	// Extract Content
	// "OK <content>\n" -> remove "OK " and trim suffix "\n"
	receivedContent := strings.TrimPrefix(getResponse, "OK ")
	receivedContent = strings.TrimSuffix(receivedContent, "\n")

	// Verify
	if receivedContent != payload {
		// Truncate for display
		dispRecv := receivedContent
		if len(dispRecv) > 50 {
			dispRecv = dispRecv[:50] + "..."
		}
		dispExp := payload
		if len(dispExp) > 50 {
			dispExp = dispExp[:50] + "..."
		}
		t.Errorf("Content mismatch!\nExpected: %s\nReceived: %s", dispExp, dispRecv)
	} else {
		t.Logf("Content verification SUCCESS. Length: %d", len(receivedContent))
	}

	t.Logf("GET Latency: %v", getDuration)
}

func TestWriteThroughput(t *testing.T) {
	// Configuration
	const clients = 10
	const duration = 10 * time.Second

	var totalWrites int64
	var totalErrors int64

	// Synchronization
	startSignal := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(clients)

	for i := 0; i < clients; i++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", "127.0.0.1:6000", 2*time.Second)
			if err != nil {
				// atomic.AddInt64(&totalErrors, 1)
				// Connection errors might happen, just skip
				return
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(duration + 5*time.Second))

			reader := bufio.NewReader(conn)

			// Try to read greeting
			conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			line, err := reader.ReadString('\n')
			conn.SetReadDeadline(time.Time{}) // Reset timeout

			isSimple := true
			if err == nil && len(line) > 0 {
				isSimple = false
			}

			<-startSignal

			endTime := time.Now().Add(duration)
			payload := generateRandomPayload(100) // Small payload for throughput

			for time.Now().Before(endTime) {
				id := uuid.New().String()

				_, err := fmt.Fprintf(conn, "SET %s %s\n", id, payload)
				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
					break
				}

				if !isSimple {
					for {
						resp, err := reader.ReadString('\n')
						if err != nil {
							atomic.AddInt64(&totalErrors, 1)
							return
						}
						if strings.HasPrefix(resp, "[SUCCESS]") || strings.HasPrefix(resp, "[ERROR]") {
							break
						}
					}
				}

				atomic.AddInt64(&totalWrites, 1)
			}
		}(i)
	}

	time.Sleep(100 * time.Millisecond)
	t.Logf("Starting throughput benchmark...")

	benchmarkStart := time.Now()
	close(startSignal)
	wg.Wait()
	benchmarkDuration := time.Since(benchmarkStart)

	writes := atomic.LoadInt64(&totalWrites)
	errs := atomic.LoadInt64(&totalErrors)
	wps := float64(writes) / benchmarkDuration.Seconds()

	t.Logf("Benchmark Complete:")
	t.Logf("Total Writes: %d", writes)
	t.Logf("Total Errors: %d", errs)
	t.Logf("Duration: %v", benchmarkDuration)
	t.Logf("Throughput: %.2f Writes/Sec", wps)
}
