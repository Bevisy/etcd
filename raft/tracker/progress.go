// Copyright 2019 The etcd Authors
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

package tracker

import (
	"fmt"
	"sort"
	"strings"
)

// Progress 代表一个 follower 在 leader 视野里的进度。Leader 维护所有 follower 的进度，并且基于此进度发送 entries 记录给 follower
//
// NB(tbg): Progress 基本上是一个状态机，它的变动散布在 `*raft.raft` 内。此外，一些字段仅在特定状态下使用。所有的这些并不理想
type Progress struct {
	// Match：对应 follower 节点当前已成功复制的 Entry 记录的索引值
	// Next：对应 follower 节点下一个待复制的 Entry 记录的索引值
	Match, Next uint64
	// State 定义 Leader 如何和 Follower 交互
	//
	// 当处于 StateProbe 状态时，leader 每个心跳间隔最多发送一条复制消息。它还会探测跟随者的实际进度
	//
	// 当处于 StateReplicate 状态时，Leader在发送复制消息后，乐观地增加最新的条目。这是一个优化状态，用于快速复制日志条目给跟随者
	//
	// 当处于 StateSnapshot 状态时，领导者应该已经发送了快照，并停止发送任何复制消息。
	State StateType

	// PendingSnapshot 被用在 StateSnapshot 中。
	// 如果有一个 pending 快照，pendingSnapshot 将被设置成快照的索引。
	// 如果 pendingSnapshot 被设置，这个 Progress 的复制进程将被暂停。raft将不会重新发送快照，直到 pending 快照被报告为失败。
	PendingSnapshot uint64

	// 如果该进度最近是活跃的，则 RecentActive 为 true。从相应的跟随者那里收到任何消息都表明该进度是活跃的。
	// 在选举超时后，RecentActive可以被重置为false。
	//
	// TODO(tbg): the leader should always have this set to true.
	RecentActive bool

	// 当这个 follower 处于 StateProbe 状态，ProbeSent 被使用。
	// 当 ProbeSent 为 true，raft 应该暂停发送复制消息到这个节点直到 ProbeSent 被重置。查看 ProbeAcked() 和 IsPaused()
	ProbeSent bool

	// Inflights 是一个滑动窗口，用于记录 inflight 消息
	// 每个 inflight 消息包含一个或者多个日志条目。
	// 每个消息的最大条目被 raft 配置 MaxSizePerMsg 定义。
	// 因此，inflight 有效的限制 inflight 消息的数量 和 每个进程可以使用的带宽。
	// 当 inflight 是满的，没法在发送更多的消息。
	// 当 leader 发送完消息，最后一个 entry 的索引应该被添加到 inflights。索引必须被顺序添加加进 inflights。
	// 当 leader 收到回复，应该通过调用 inflights.FreeLE 和最后收到的条目的索引来释放之前的 inflights。
	Inflights *Inflights

	// IsLearner 为 true 如果这个进程为 learner 而被跟踪
	IsLearner bool
}

// ResetState moves the Progress into the specified State, resetting ProbeSent,
// PendingSnapshot, and Inflights.
func (pr *Progress) ResetState(state StateType) {
	pr.ProbeSent = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.Inflights.reset()
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

// ProbeAcked is called when this peer has accepted an append. It resets
// ProbeSent to signal that additional append messages should be sent without
// further delay.
func (pr *Progress) ProbeAcked() {
	pr.ProbeSent = false
}

// BecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
// optionally and if larger, the index of the pending snapshot.
func (pr *Progress) BecomeProbe() {
	// If the original state is StateSnapshot, progress knows that
	// the pending snapshot has been sent to this peer successfully, then
	// probes from pendingSnapshot + 1.
	if pr.State == StateSnapshot {
		pendingSnapshot := pr.PendingSnapshot
		pr.ResetState(StateProbe)
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.ResetState(StateProbe)
		pr.Next = pr.Match + 1
	}
}

// BecomeReplicate() 使进程状态过渡到 StateReplicate，重置 Next 为 Match+1
func (pr *Progress) BecomeReplicate() {
	pr.ResetState(StateReplicate)
	pr.Next = pr.Match + 1
}

// BecomeSnapshot moves the Progress to StateSnapshot with the specified pending
// snapshot index.
func (pr *Progress) BecomeSnapshot(snapshoti uint64) {
	pr.ResetState(StateSnapshot)
	pr.PendingSnapshot = snapshoti
}

// MaybeUpdate is called when an MsgAppResp arrives from the follower, with the
// index acked by it. The method returns false if the given n index comes from
// an outdated message. Otherwise it updates the progress and returns true.
func (pr *Progress) MaybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n // n 之前的成功发送所有 Entry 记录 已经写入对应节点的 raftLog 中
		updated = true
		pr.ProbeAcked() // 设置 ProbeSent = false, 提示可以继续向节点发送 Entry
	}
	if pr.Next < n+1 {
		pr.Next = n + 1 // 设置 Next字段为下次要复制的 Entry 记录的索引
	}
	return updated
}

// OptimisticUpdate signals that appends all the way up to and including index n
// are in-flight. As a result, Next is increased to n+1.
func (pr *Progress) OptimisticUpdate(n uint64) { pr.Next = n + 1 }

// MaybeDecrTo 根据收到的 MsgApp 中拒绝的参数调整 Progress 中的 Next 。参数是跟随者拒绝附加到其日志的索引，以及其最后的索引
//
// Rejections can happen spuriously as messages are sent out of order or
// duplicated. In such cases, the rejection pertains to an index that the
// Progress already knows were previously acknowledged, and false is returned
// without changing the Progress.
// 拒绝可能会虚假地发生，因为消息被不按顺序发送或重复。在这种情况下，拒绝涉及到Progress已经知道以前被确认的索引，并且返回false而不改变Progress
//
// If the rejection is genuine, Next is lowered sensibly, and the Progress is
// cleared for sending log entries.
// 如果拒绝是真实的， Next 会被合理地降低，Profress 会被清除以发送日志条目。
//
// rejected: 被拒绝消息 MsgApp 的 Index
// last: 对应 follower 节点 raftLog 最有一条 Index
func (pr *Progress) MaybeDecrTo(rejected, last uint64) bool {
	if pr.State == StateReplicate {
		// The rejection must be stale(陈旧的) if the progress has matched and "rejected"
		// is smaller than "match".
		if rejected <= pr.Match { // 过时的 MsgApp 消息，直接忽略
			return false
		}
		// Directly decrease next to match + 1.
		//
		// TODO(tbg): why not use last if it's larger?
		// 处于 StateReplicate 状态时，发送 MsgApp 会调用 Process.optimisticUpfate()方法增加 Next，这会导致 Next 会大 Match 很多，这里直接回退 Next 到 Match+1，后续重新发送消息 MsgApp 进行尝试
		pr.Next = pr.Match + 1
		return true
	}

	// The rejection must be stale if "rejected" does not match next - 1. This
	// is because non-replicating followers are probed one entry at a time.
	if pr.Next-1 != rejected { // 过时的 MsgAppResp 消息，直接忽略
		return false
	}

	if pr.Next = min(rejected, last+1); pr.Next < 1 { // 根据 MsgAppResp 重置 Next
		pr.Next = 1
	}
	pr.ProbeSent = false // 恢复消息发送
	return true
}

// IsPaused 返回是否发送日志记录到这个节点已经被节制
// This is done when a node has rejected recent MsgApps, is currently waiting
// for a snapshot, or has reached the MaxInflightMsgs limit. In normal
// operation, this is false. A throttled node will be contacted less frequently
// until it has reached a state in which it's able to accept a steady stream of
// log entries again.
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case StateProbe:
		return pr.ProbeSent
	case StateReplicate:
		return pr.Inflights.Full()
	case StateSnapshot:
		return true
	default:
		panic("unexpected state")
	}
}

func (pr *Progress) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%s match=%d next=%d", pr.State, pr.Match, pr.Next)
	if pr.IsLearner {
		fmt.Fprint(&buf, " learner")
	}
	if pr.IsPaused() {
		fmt.Fprint(&buf, " paused")
	}
	if pr.PendingSnapshot > 0 {
		fmt.Fprintf(&buf, " pendingSnap=%d", pr.PendingSnapshot)
	}
	if !pr.RecentActive {
		fmt.Fprintf(&buf, " inactive")
	}
	if n := pr.Inflights.Count(); n > 0 {
		fmt.Fprintf(&buf, " inflight=%d", n)
		if pr.Inflights.Full() {
			fmt.Fprint(&buf, "[full]")
		}
	}
	return buf.String()
}

// ProgressMap is a map of *Progress.
type ProgressMap map[uint64]*Progress

// String prints the ProgressMap in sorted key order, one Progress per line.
func (m ProgressMap) String() string {
	ids := make([]uint64, 0, len(m))
	for k := range m {
		ids = append(ids, k)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	var buf strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&buf, "%d: %s\n", id, m[id])
	}
	return buf.String()
}
