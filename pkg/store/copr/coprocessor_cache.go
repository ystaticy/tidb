// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package copr

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/dgraph-io/ristretto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	copr_metrics "github.com/pingcap/tidb/pkg/store/copr/metrics"
	"github.com/tikv/client-go/v2/config"
)

type coprCache struct {
	cache                   *ristretto.Cache
	admissionMaxRanges      int
	admissionMaxSize        int
	admissionMinProcessTime time.Duration
}

type coprCacheValue struct {
	Key               []byte
	Data              []byte
	TimeStamp         uint64
	RegionID          uint64
	RegionDataVersion uint64

	// Used in coprocessor paging protocol
	PageStart []byte
	PageEnd   []byte
}

func (v *coprCacheValue) String() string {
	return fmt.Sprintf("{ Ts = %d, RegionID = %d, RegionDataVersion = %d, len(Data) = %d }",
		v.TimeStamp,
		v.RegionID,
		v.RegionDataVersion,
		len(v.Data))
}

const coprCacheValueSize = int(unsafe.Sizeof(coprCacheValue{}))

func (v *coprCacheValue) Len() int {
	return coprCacheValueSize + len(v.Key) + len(v.Data) + len(v.PageStart) + len(v.PageEnd)
}

func newCoprCache(config *config.CoprocessorCache) (*coprCache, error) {
	if config == nil || config.CapacityMB == 0 {
		return nil, nil
	}
	capacityInBytes := int64(config.CapacityMB * 1024.0 * 1024.0)
	if capacityInBytes <= 0 {
		return nil, errors.New("Capacity must be > 0 to enable the cache")
	}
	maxEntityInBytes := int64(config.AdmissionMaxResultMB * 1024.0 * 1024.0)
	if maxEntityInBytes == 0 {
		return nil, errors.New("AdmissionMaxResultMB must be > 0 to enable the cache")
	}
	estimatedEntities := max(capacityInBytes/maxEntityInBytes*2, 10)
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: estimatedEntities * 10,
		MaxCost:     capacityInBytes,
		BufferItems: 64,
		OnEvict: func(_ *ristretto.Item) {
			copr_metrics.CoprCacheCounterEvict.Add(1)
		},
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	c := coprCache{
		cache:                   cache,
		admissionMaxRanges:      int(config.AdmissionMaxRanges),
		admissionMaxSize:        int(maxEntityInBytes),
		admissionMinProcessTime: time.Duration(config.AdmissionMinProcessMs) * time.Millisecond,
	}
	return &c, nil
}

func coprCacheBuildKey(copReq *coprocessor.Request) ([]byte, error) {
	// Calculate amount of space to allocate
	if copReq.Tp > math.MaxUint8 {
		return nil, errors.New("Request Tp too big")
	}
	if len(copReq.Data) > math.MaxUint32 {
		return nil, errors.New("Cache data too big")
	}
	totalLength := 1 + 4 + len(copReq.Data)
	for _, r := range copReq.Ranges {
		if len(r.Start) > math.MaxUint16 {
			return nil, errors.New("Cache start key too big")
		}
		if len(r.End) > math.MaxUint16 {
			return nil, errors.New("Cache end key too big")
		}
		totalLength += 2 + len(r.Start) + 2 + len(r.End)
	}
	if copReq.PagingSize > 0 {
		totalLength++
	}

	key := make([]byte, totalLength)

	// 1 byte Tp
	key[0] = uint8(copReq.Tp)
	dest := 1

	// 4 bytes Data len
	binary.LittleEndian.PutUint32(key[dest:], uint32(len(copReq.Data)))
	dest += 4

	// N bytes Data
	copy(key[dest:], copReq.Data)
	dest += len(copReq.Data)

	for _, r := range copReq.Ranges {
		// 2 bytes Key len
		binary.LittleEndian.PutUint16(key[dest:], uint16(len(r.Start)))
		dest += 2

		// N bytes Key
		copy(key[dest:], r.Start)
		dest += len(r.Start)

		// 2 bytes Key len
		binary.LittleEndian.PutUint16(key[dest:], uint16(len(r.End)))
		dest += 2

		// N bytes Key
		copy(key[dest:], r.End)
		dest += len(r.End)
	}

	// 1 byte when use paging protocol
	if copReq.PagingSize > 0 {
		key[dest] = 1
	}

	return key, nil
}

// Get gets a cache item according to cache key.
func (c *coprCache) Get(key []byte) *coprCacheValue {
	if c == nil {
		return nil
	}
	value, hit := c.cache.Get(key)
	if !hit {
		return nil
	}
	typedValue := value.(*coprCacheValue)
	// ristretto does not handle hash collision, so check the key equality after getting a value.
	if !bytes.Equal(typedValue.Key, key) {
		return nil
	}
	return typedValue
}

// CheckRequestAdmission checks whether a response item is worth caching.
func (c *coprCache) CheckRequestAdmission(ranges int) bool {
	if c == nil {
		return false
	}
	if c.admissionMaxRanges != 0 && ranges > c.admissionMaxRanges {
		return false
	}
	return true
}

// CheckResponseAdmission checks whether a response item is worth caching.
func (c *coprCache) CheckResponseAdmission(dataSize int, processTime time.Duration, pagingTaskIdx uint32) bool {
	if c == nil {
		return false
	}
	if dataSize == 0 || dataSize > c.admissionMaxSize {
		return false
	}
	if pagingTaskIdx > 50 {
		return false
	}

	admissionMinProcessTime := c.admissionMinProcessTime
	if pagingTaskIdx > 0 {
		admissionMinProcessTime = admissionMinProcessTime / 3
	}
	if processTime < admissionMinProcessTime {
		return false
	}
	return true
}

// Set inserts an item to the cache.
// It is recommended to call `CheckRequestAdmission` and `CheckResponseAdmission` before inserting
// the item to the cache.
func (c *coprCache) Set(key []byte, value *coprCacheValue) bool {
	if c == nil {
		return false
	}
	// Always ensure that the `Key` in `value` is the `key` we received.
	value.Key = key
	return c.cache.Set(key, value, int64(value.Len()))
}
