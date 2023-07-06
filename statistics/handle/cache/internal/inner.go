// Copyright 2023 PingCAP, Inc.
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

package internal

import (
	"github.com/pingcap/tidb/statistics"
)

// StatsCacheInner is the interface to manage the statsCache, it can be implemented by map, lru cache or other structures.
type StatsCacheInner interface {
	// GetByQuery retrieves cache triggered by a query. Usually used in LRU.
	GetByQuery(int64) (*statistics.Table, bool)
	// Get gets the cache.
	Get(int64) (*statistics.Table, bool)
	// PutByQuery puts a cache triggered by a query. Usually used in LRU.
	PutByQuery(int64, *statistics.Table)
	// Put puts a cache.
	Put(int64, *statistics.Table)
	// Del deletes a cache.
	Del(int64)
	// Cost returns the memory usage of the cache.
	Cost() int64
	// Keys returns the keys of the cache.
	Keys() []int64
	// Values returns the values of the cache.
	Values() []*statistics.Table
	Map() map[int64]*statistics.Table
	// Len returns the length of the cache.
	Len() int
	FreshMemUsage()
	// Copy returns a copy of the cache
	Copy() StatsCacheInner
	// SetCapacity sets the capacity of the cache
	SetCapacity(int64)
	// Front returns the front element's owner tableID, only used for test
	Front() int64
}

// COWStatsCache is a copy-on-write stats cache.
type COWStatsCache interface {
	// GetByQuery retrieves cache triggered by a query. Usually used in LRU.
	GetByQuery(int64) (*statistics.Table, bool)
	// Get gets the cache.
	Get(int64) (*statistics.Table, bool)
	// Cost returns the memory usage of the cache.
	Cost() int64
	// Update creates a new COWStatsCache and updates on the new cache and returns it.
	Update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64, byQuery bool) COWStatsCache
	// Version returns the version of the cache.
	// initVersion is the version set when initializing this cache.
	// updateCounter is the number of update operation on this cache.
	Version() (initVersion, updateCounter uint64)
}

type cowStatsCache struct {
	inner         StatsCacheInner
	initVersion   uint64
	updateCounter uint64
}

// NewCOWStatsCache creates a new COWStatsCache.
func NewCOWStatsCache(c StatsCacheInner, version uint64) COWStatsCache {
	return &cowStatsCache{
		inner:         c,
		initVersion:   version,
		updateCounter: 0,
	}
}

// GetByQuery implements the GetByQuery method.
func (c *cowStatsCache) GetByQuery(tableID int64) (*statistics.Table, bool) {
	return c.inner.GetByQuery(tableID)
}

// Get implements the Get method.
func (c *cowStatsCache) Get(tableID int64) (*statistics.Table, bool) {
	return c.inner.Get(tableID)
}

// Cost implements the Cost method.
func (c *cowStatsCache) Cost() int64 {
	return c.inner.Cost()
}

// Update implements the Update method.
func (c *cowStatsCache) Update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64, byQuery bool) COWStatsCache {
	newCache := &cowStatsCache{
		inner:         c.inner.Copy(),
		initVersion:   c.initVersion,
		updateCounter: c.updateCounter + 1,
	}
	for _, tbl := range tables {
		id := tbl.PhysicalID
		if byQuery {
			newCache.inner.PutByQuery(id, tbl)
		} else {
			newCache.inner.Put(id, tbl)
		}
	}
	for _, id := range deletedIDs {
		newCache.inner.Del(id)
	}
	return newCache
}

// Version implements the Version method.
func (c *cowStatsCache) Version() (initVersion, updateCounter uint64) {
	return c.initVersion, c.updateCounter
}
