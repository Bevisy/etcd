// Copyright 2015 The etcd Authors
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

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/etcdserver/api/snap"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"

	"go.uber.org/zap"
)

// A key-value stream backed by raft
type raftNode struct {
	proposeC    <-chan string            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- *string           // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session

	id          int      // client ID for raft session
	peers       []string // raft peer URLs
	join        bool     // node is joining an existing cluster
	waldir      string   // path to WAL directory
	snapdir     string   // path to snapshot directory
	getSnapshot func() ([]byte, error)
	lastIndex   uint64 // index of log at start

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
}

var defaultSnapshotCount uint64 = 10000

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit chanel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
//
// newRaftNode 初始化一个 raft 实例并返回一个提交的日志记录条目通道和错误通道。
// 日志提案更新在被提供的提案通道中被发送。
// 所有的位于 commit 通道的日志记录被重放
// 关闭 proposeC 和 读取 errorC 以关闭节点
func newRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan *string, <-chan error, <-chan *snap.Snapshotter) {

	// 创建 commutC、errorC 通道
	commitC := make(chan *string)
	errorC := make(chan error)

	// 初始化 raftNode 部分字段
	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		waldir:      fmt.Sprintf("raftexample-%d", id),
		snapdir:     fmt.Sprintf("raftexample-%d-snap", id),
		getSnapshot: getSnapshot,
		snapCount:   defaultSnapshotCount,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),

		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}
	// 启动协程以执行 startRaft()，在其中完成剩余初始化操作
	go rc.startRaft()
	return commitC, errorC, rc.snapshotterReady
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := walpb.Snapshot{ // 根据快照元数据创建 walpb.Snapshot 实例
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	// WAL 会将上述快照的元数据信息封装成一条日志记录下来
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	// 将新的快照数据写入快照文件
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	// 根据快照的元数据信息，释放无用的 wal 文件句柄
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

// 校验并返回实际需要被应用的 entries
func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 { // 待应用的 ents 为空，直接返回
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 { // 判断待应用的记录需要与 appliedIndex 相等或者更小
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) { // 过滤已被应用的记录
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
//
// publishEntries 将已经被提交的日志记录写入 commitC 通道，并且返回是否全部的记录可以被发布
func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			select {
			case rc.commitC <- &s:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type { // 节点变更信息类型
			case raftpb.ConfChangeAddNode: // 增加节点
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode: // 移出节点
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == rc.lastIndex {
			select {
			case rc.commitC <- nil:
			case <-rc.stopc:
				return false
			}
		}
	}
	return true
}

func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load() // 快照文件读取
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), rc.waldir, nil) // 新建 wal 实例，并创建一个空的 wal 日志文件
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{} // 创建 walsnap.Snapshot 实例，只包含快照索引和任期，不包含数据
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term // 根据快照信息构建 WAL 实例
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap) // 根据快照信息打开 wal 文件，创建 wal 实例
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
// 重放 wal 记录到 raft 实例
func (rc *raftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)
	snapshot := rc.loadSnapshot()   // 读取 snapshot
	w := rc.openWAL(snapshot)       // 根据读取到的 snapshot 实例元数据构建 WAL 实例
	_, st, ents, err := w.ReadAll() // 读取快照信息之后的 wal 日志，并获取状态信息
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	rc.raftStorage = raft.NewMemoryStorage() // 创建持久化存储实例 MemoryStorage
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot) // 加载快照数据到 MemoryStorage 中
	}
	rc.raftStorage.SetHardState(st) // 将读取 wal 日志后得到的 HardState 加载到 MemoryStorage 中

	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents) // 将读取 wal 日志中的 ents 追加到 MemoryStorage 中，这样 raft 将在正确的日志位置开始
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 { // 快照之后存在已经持久化的 Entry 记录，这些记录需要回放到上层应用的状态机中
		rc.lastIndex = ents[len(ents)-1].Index // 更新 raft.lastIndex，记录回放结束位置
	} else {
		rc.commitC <- nil // 快照之后不存在已持久化的 Entry 记录，则向 commitC 中写入 nil 作为信号
	}
	return w
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) startRaft() {
	// 判断 snapdir 是否存在，不存在则创建
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	// 1. 创建 Snapshotter 实例，并将 Snapshotter 实例通过 snapshotterReady 通道返回给上层应用。SnapShotter 实例提供了读写快照文件的功能
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir) // 初始化 snapshotter
	rc.snapshotterReady <- rc.snapshotter                   // 写入 snapshotterReady 通道，返回给上层应用

	// 2. 创建 WAL 实例，加载快照并回放 WAL 日志
	oldwal := wal.Exist(rc.waldir) // 判断是否存在旧的 wal 文件
	rc.wal = rc.replayWAL()        // 加载快照 snapshot，并重放 wal 记录

	// 3. 创建 raft.Config 实例
	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage, // 持久化存储
		MaxSizePerMsg:             1024 * 1024,    // 每条消息最大长度
		MaxInflightMsgs:           256,            // 已发送，但未收到响应的消息上限数
		MaxUncommittedEntriesSize: 1 << 30,        // 最大未提交的记录容量上限
	}

	// 4. 初始化底层 etcd-raft 模块，会辨别是初次启动或重启
	if oldwal {
		rc.node = raft.RestartNode(c) // 重启
	} else {
		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers) // 初次启动
	}

	// 5. 创建 transport 实例并启动，该实例负责集群中各个节点之间的网络通信
	rc.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start() // 启动网络服务相关组件

	// 6. 建立与集群其它节点的连接
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	// 7. 启动协程以监听当前节点与集群中其它节点之间的网络连接
	go rc.serveRaft()

	// 8. 启动协程以处理上层应用与底层 etcd-raft 模块的交互
	go rc.serveChannels()
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	// 使用 commitC 通道通知上层应用加载新生成的快照数据
	rc.commitC <- nil // trigger kvstore to load snapshot

	// 记录新快照的元数据
	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *raftNode) maybeTriggerSnapshot() {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	if err := rc.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
}

func (rc *raftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot() // 获取快照数据和快照元数据
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond) // 创建 100ms 间隔的定时器
	defer ticker.Stop()

	// send proposals over raft
	// 启动协程负责将 proposeC、confChangeC 通道上接收到的数据传递给 etcd-raft 组件进行处理
	go func() {
		confChangeCount := uint64(0)

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil // 发生异常，则循环退出
				} else {
					// blocks until accepted by raft state machine
					rc.node.Propose(context.TODO(), []byte(prop)) // 处理写请求，交给 etcd-raft 模块
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil // 发生异常，则循环退出
				} else {
					confChangeCount++ // 统计集群变更请求数，并将其作为 ID
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc) // 处理配置变更请求，交给 etcd-raft 模块处理
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc) // 关闭 stopc 通道，触发 rafeNode.stop()
	}()

	// event loop on raft state machine updates
	// 处理底层 etcd-raft 模块返回的 Ready 数据
	for {
		select {
		case <-ticker.C:
			rc.node.Tick() // 推进逻辑时钟

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready(): // 获取 Ready 数据
			rc.wal.Save(rd.HardState, rd.Entries) // 将获取的 Ready 实例中的 状态和待持久化的记录记录到 WAL 日志文件
			if !raft.IsEmptySnap(rd.Snapshot) {   // 检测是否存在新的快照数据
				rc.saveSnap(rd.Snapshot)                  // 将新的快照数据写入快照文件中
				rc.raftStorage.ApplySnapshot(rd.Snapshot) // 将新的快照数据持久化到 raftStorage.MemoryStorage 中
				rc.publishSnapshot(rd.Snapshot)           // 通知上层应用加载新的快照
			}
			rc.raftStorage.Append(rd.Entries)                                         // 将待持久化的 entries 追加到 raftStorage 中完成持久化
			rc.transport.Send(rd.Messages)                                            // 将待发送的消息发送到指定节点
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok { // 将已提交、待应用的 Entry 记录应用到上层应用的状态机中
				rc.stop()
				return
			}
			rc.maybeTriggerSnapshot() // 触发快照
			rc.node.Advance()

		case err := <-rc.transport.ErrorC: // 处理网络异常
			rc.writeError(err) // 关闭集群中与其它节点的连接
			return

		case <-rc.stopc: // 处理关闭信号
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.peers[rc.id-1]) // 获取当前节点 url 地址
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	// 创建 stoppableListener 实例，stoppableListener 继承 net.TCPListener（实现了 net.Listener 接口）接口，它会与 http.Server 配合实现对当前节点的 URL 地址进行监听
	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	// 创建 http.Server 实例，它会通过上面的 stoppableListener 实例监听当前节点的 URL 地址 stoppableListener.Accept() 方法监听到新连接到来时，会创建对应的 net.Conn 实例，http.Server 会为每个连接创建单独的 goroutine 处理，每个请求都会由 http.Server.Handler 处理。这里的 handler 是由 rafthttp.Transporter 创建的。http.Server.Serve()方法会一直阻塞，直到 http.Server 关闭
	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
