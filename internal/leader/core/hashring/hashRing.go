package hashring

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/netip"
	"slices"
	"strconv"
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
	id_map    *sync.Map
	ring      atomic.Value
	mu        sync.Mutex
	sfg       singleflight.Group
	vnodes    int
}

func NewHashRing(vnodeCount int) *HashRing {
	hr := &HashRing{
		addr_list: &sync.Map{},
		conn_list: &sync.Map{},
		id_map:    &sync.Map{},
		vnodes:    vnodeCount,
	}
	empty := make([]string, 0)
	hr.ring.Store(empty)
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
		return errors.New("Invalid node_id, must be a UUID string")
	}
	if _, err := netip.ParseAddrPort(node_addr); err != nil {
		return errors.New("Invalid node_addr, must be an ip + port adress string like '<ip>:<port>' ")
	}
	hr.mu.Lock()
	defer hr.mu.Unlock()

	oldRing := hr.ring.Load().([]string)
	newRing := slices.Clone(oldRing)

	vnodeHashes := make([]string, 0, hr.vnodes)

	for i := 0; i < hr.vnodes; i++ {

		vnodeKey := node_id + "-" + strconv.Itoa(i)
		hash := sha256.Sum256([]byte(vnodeKey))
		vnodeHash := hex.EncodeToString(hash[:])

		newRing = append(newRing, vnodeHash)
		vnodeHashes = append(vnodeHashes, vnodeHash)

		hr.addr_list.Store(vnodeHash, node_addr)
	}

	slices.Sort(newRing)
	hr.ring.Store(newRing)
	hr.id_map.Store(node_id, vnodeHashes)

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
	if len(ring_instance) == 0 {
		return -1
	}
	return (current_index + 1) % len(ring_instance)
}

func (hr *HashRing) RemoveNode(nodeID string) error {
	val, ok := hr.id_map.Load(nodeID)
	if !ok {
		return errors.New("node not found")
	}
	vnodeHashes := val.([]string)

	hr.mu.Lock()
	defer hr.mu.Unlock()

	oldRing := hr.ring.Load().([]string)

	toRemove := make(map[string]struct{})
	for _, h := range vnodeHashes {
		toRemove[h] = struct{}{}
		hr.addr_list.Delete(h)
	}
	newRing := make([]string, 0, len(oldRing)-hr.vnodes)
	for _, h := range oldRing {
		if _, found := toRemove[h]; !found {
			newRing = append(newRing, h)
		}
	}

	hr.ring.Store(newRing)
	hr.id_map.Delete(nodeID)

	return nil
}
func (hr *HashRing) GetCurrentMembers() ([]string, []string) {
	physicalIDs := make([]string, 0)
	physicalIPs := make([]string, 0)

	hr.id_map.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		vnodeHashes := value.([]string)
		if len(vnodeHashes) > 0 {
			if addr, ok := hr.addr_list.Load(vnodeHashes[0]); ok {
				physicalIDs = append(physicalIDs, nodeID)
				physicalIPs = append(physicalIPs, addr.(string))
			}
		}
		return true
	})

	return physicalIDs, physicalIPs
}
func (hr *HashRing) GetNodeCount() int {
	count := 0
	hr.id_map.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}
