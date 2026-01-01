package hashring

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type HashRing struct {
	addr_list *sync.Map
	conn_list *sync.Map
	ring      atomic.Value
	mu        sync.Mutex
	sfg       singleflight.Group
}

func NewHashRing() *HashRing {
	hr := &HashRing{}
	empty := make([]string, 0)
	hr.ring.Store(&empty)
	return hr
}

func (hr *HashRing) FindContainingNodeIndex(file_id string) (int, error) {
	if _, err := uuid.Parse(file_id); err != nil {
		return -1, errors.New("Invalid file_id, must be a UUID")
	}

	ring_instance := hr.ring.Load().([]string)
	if len(ring_instance) == 0 {
		return -1, errors.New("ring is empty")
	}

	hash := sha256.Sum256([]byte(file_id))
	hexHash := hex.EncodeToString(hash[:])
	index, _ := slices.BinarySearchFunc(ring_instance, hexHash, strings.Compare)
	if index == len(ring_instance) {
		index = 0
	}

	return index, nil
}

func (hr *HashRing) AddNode(node_id string, node_addr string) error {
	if _, err := uuid.Parse(node_id); err != nil {
		return errors.New("Invalid file_id, must be a UUID string")
	}
	if _, err := netip.ParseAddrPort(node_addr); err != nil {
		return errors.New("Invalid node_addr, must be an ip + port adress string like '<ip>:<port>' ")
	}
	hr.mu.Lock()
	defer hr.mu.Unlock()
	oldRing := hr.ring.Load().([]string)
	newHash := sha256.Sum256([]byte(node_id))
	newNodeHash := hex.EncodeToString(newHash[:])

	newRing := append(slices.Clone(oldRing), string(newNodeHash))
	slices.Sort(newRing)
	hr.ring.CompareAndSwap(oldRing, newRing)

	hr.addr_list.Store(newNodeHash, node_addr)
	return nil
}

func (hr *HashRing) GetOrCreateConnection(addr string) (*grpc.ClientConn, error) {
	if _, err := netip.ParseAddrPort(addr); err != nil {
		return nil, errors.New("Invalid node_addr, must be an ip + port adress string like '<ip>:<port>' ")
	}
	if val, ok := hr.conn_list.Load(addr); ok {
		return val.(*grpc.ClientConn), nil
	}

	val, err, _ := hr.sfg.Do(addr, func() (interface{}, error) {
		if val, ok := hr.conn_list.Load(addr); ok {
			return val, nil
		}

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}

		hr.conn_list.Store(addr, conn)
		return conn, nil
	})
	if err != nil {
		return nil, err
	}

	return val.(*grpc.ClientConn), nil
}

func (hr *HashRing) GetAddrFromIndex(index int) (string, error) {
	ring_instance := hr.ring.Load().([]string)
	host_id := ring_instance[index]

	addr, ok := hr.addr_list.Load(host_id)
	if !ok {
		return "", errors.New("Couldn't find adress on selected index")
	}
	return addr.(string), nil
}

func (hr *HashRing) Walk(current_index int) (next_index int) {
	if current_index <= -1 {
		return -1
	}
	ring_instance := hr.ring.Load().([]string)

	return (current_index + 1) % len(ring_instance)
}

func (hr *HashRing) RemoveNode(nodeID string) error {
	if _, err := uuid.Parse(nodeID); err != nil {
		return errors.New("invalid node_id, must be a UUID string")
	}

	hash := sha256.Sum256([]byte(nodeID))
	nodeHash := hex.EncodeToString(hash[:])

	hr.mu.Lock()
	defer hr.mu.Unlock()

	oldRing := hr.ring.Load().([]string)

	idx := -1
	for i, v := range oldRing {
		if v == nodeHash {
			idx = i
			break
		}
	}

	if idx == -1 {
		return errors.New("node not found in ring")
	}

	newRing := slices.Delete(slices.Clone(oldRing), idx, idx+1)

	hr.ring.Store(newRing)

	if addr, ok := hr.addr_list.LoadAndDelete(nodeHash); ok {
		addrStr := addr.(string)

		if val, ok := hr.conn_list.LoadAndDelete(addrStr); ok {
			if conn, ok := val.(*grpc.ClientConn); ok {
				_ = conn.Close()
			}
		}
	}

	return nil
}
