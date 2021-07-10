// Copyright 2016 The etcd Authors
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

package contention

import (
	"sync"
	"time"
)

// TimeoutDetector detects routine starvations by
// observing the actual time duration to finish an action
// or between two events that should happen in a fixed
// interval. If the observed duration is longer than
// the expectation, the detector will report the result.
type TimeoutDetector struct {
	mu          sync.Mutex // protects all
	maxDuration time.Duration
	records     map[uint64]time.Time // 该 map 中记录了上一次向目标节点发送心跳消息的时间 (key是节点 ID, value是具体时间)
}

// NewTimeoutDetector creates the TimeoutDetector.
func NewTimeoutDetector(maxDuration time.Duration) *TimeoutDetector {
	return &TimeoutDetector{
		maxDuration: maxDuration,
		records:     make(map[uint64]time.Time),
	}
}

// Reset resets the NewTimeoutDetector.
func (td *TimeoutDetector) Reset() {
	td.mu.Lock()
	defer td.mu.Unlock()

	td.records = make(map[uint64]time.Time)
}

// Observe observes an event for given id. It returns false and exceeded duration
// if the interval is longer than the expectation.
//
// 如果两次心跳发送事件间隔超时，则返回 false 和 超时部分的时间
func (td *TimeoutDetector) Observe(which uint64) (bool, time.Duration) {
	td.mu.Lock()
	defer td.mu.Unlock()

	ok := true
	now := time.Now()
	exceed := time.Duration(0)

	if pt, found := td.records[which]; found {
		exceed = now.Sub(pt) - td.maxDuration // 计算两次发送心跳间隔: 两次心跳发送事件间隔 - 期望值（两倍心跳时间）
		if exceed > 0 {
			ok = false
		}
	}
	td.records[which] = now // 重置节点发送心跳事件时间
	return ok, exceed
}
