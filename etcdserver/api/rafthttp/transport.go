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

package rafthttp

import (
	"context"
	"net/http"
	"sync"
	"time"

	"go.etcd.io/etcd/etcdserver/api/snap"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/logutil"
	"go.etcd.io/etcd/pkg/transport"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"

	"github.com/coreos/pkg/capnslog"
	"github.com/xiang90/probing"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var plog = logutil.NewMergeLogger(capnslog.NewPackageLogger("go.etcd.io/etcd", "rafthttp"))

type Raft interface {
	// 将指定的消息实例传递到底层的 etcd-raft 模块进行处理
	Process(ctx context.Context, m raftpb.Message) error
	// 检测指定节点是否从当前集群中移出
	IsIDRemoved(id uint64) bool
	// 通知底层 etcd-raft 模块，当前节点与指定节点无法连通
	ReportUnreachable(id uint64)
	// 通知底层 etcd-raft 模块，快照数据是否发送成功
	ReportSnapshot(id uint64, status raft.SnapshotStatus)
}

type Transporter interface {
	// Start starts the given Transporter.
	// Start MUST be called before calling other functions in the interface.
	//
	// 初始化操作
	Start() error
	// Handler returns the HTTP handler of the transporter.
	// A transporter HTTP handler handles the HTTP requests
	// from remote peers.
	// The handler MUST be used to handle RaftPrefix(/raft)
	// endpoint.
	//
	// 创建 Handler 实例，并关联 URL
	Handler() http.Handler
	// Send sends out the given messages to the remote peers.
	// Each message has a To field, which is an id that maps
	// to an existing peer in the transport.
	// If the id cannot be found in the transport, the message
	// will be ignored.
	//
	// 发送消息
	Send(m []raftpb.Message)
	// SendSnapshot sends out the given snapshot message to a remote peer.
	// The behavior of SendSnapshot is similar to Send.
	//
	// 发送快照数据
	SendSnapshot(m snap.Message)
	// AddRemote adds a remote with given peer urls into the transport.
	// A remote helps newly joined member to catch up the progress of cluster,
	// and will not be used after that.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	//
	// 在集群中添加节点时，其它节点通过该方法添加新节点
	AddRemote(id types.ID, urls []string)
	// AddPeer adds a peer with given peer urls into the transport.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	// Peer urls are used to connect to the remote peer.
	AddPeer(id types.ID, urls []string)
	// RemovePeer removes the peer with given id.
	RemovePeer(id types.ID)
	// RemoveAllPeers removes all the existing peers in the transport.
	RemoveAllPeers()
	// UpdatePeer updates the peer urls of the peer with the given id.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	UpdatePeer(id types.ID, urls []string)
	// ActiveSince returns the time that the connection with the peer
	// of the given id becomes active.
	// If the connection is active since peer was added, it returns the adding time.
	// If the connection is currently inactive, it returns zero time.
	ActiveSince(id types.ID) time.Time
	// ActivePeers returns the number of active peers.
	ActivePeers() int
	// Stop closes the connections and stops the transporter.
	//
	// 关闭全部网络连接
	Stop()
}

// Transport implements Transporter interface. It provides the functionality
// to send raft messages to peers, and receive raft messages from peers.
// User should call Handler method to get a handler to serve requests
// received from peerURLs.
// User needs to call Start before calling other functions, and call
// Stop when the Transport is no longer used.
type Transport struct {
	Logger *zap.Logger

	DialTimeout time.Duration // maximum duration before timing out dial of the request
	// DialRetryFrequency defines the frequency of streamReader dial retrial attempts;
	// a distinct rate limiter is created per every peer (default value: 10 events/sec)
	DialRetryFrequency rate.Limit

	TLSInfo transport.TLSInfo // TLS information used when creating connection

	ID          types.ID   // local member ID
	URLs        types.URLs // local peer URLs
	ClusterID   types.ID   // raft cluster ID for request validation
	Raft        Raft       // raft state machine, to which the Transport forwards received messages and reports status
	Snapshotter *snap.Snapshotter
	ServerStats *stats.ServerStats // used to record general transportation statistics
	// used to record transportation statistics with followers when
	// performing as leader in raft protocol
	LeaderStats *stats.LeaderStats
	// ErrorC is used to report detected critical errors, e.g.,
	// the member has been permanently removed from the cluster
	// When an error is received from ErrorC, user should stop raft state
	// machine and thus stop the Transport.
	ErrorC chan error

	streamRt   http.RoundTripper // roundTripper used by streams
	pipelineRt http.RoundTripper // roundTripper used by pipelines

	mu      sync.RWMutex         // protect the remote and peer map
	remotes map[types.ID]*remote // remote 主要负责发送快照数据，帮助新加入的节点快速追赶上其他节点的数据
	peers   map[types.ID]Peer    // peers map

	pipelineProber probing.Prober // 探测 pipeline 消息通道是否可用
	streamProber   probing.Prober // 探测 stream 消息通道是否可用
}

// Transport 实例初始化
func (t *Transport) Start() error {
	var err error
	// 创建 Stream 消息通道使用的 http.RoundTripper 实例，底层实际上是创建 http.Transport 实例。注意：创建连接的超时时间（根据配置指定） 、读写请求的超时时间（默认 5s）和 keepAlive 时间（默认为 30s)
	t.streamRt, err = newStreamRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return err
	}
	// 创建 Pipeline 消息通道使用的 http.RoundTripper 实例。与 streamRt 不同的是，读写请求的超时时间设置成了永不过期
	t.pipelineRt, err = NewRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return err
	}
	t.remotes = make(map[types.ID]*remote)
	t.peers = make(map[types.ID]Peer)
	t.pipelineProber = probing.NewProber(t.pipelineRt)
	t.streamProber = probing.NewProber(t.streamRt)

	// If client didn't provide dial retry frequency, use the default
	// (100ms backoff between attempts to create a new stream),
	// so it doesn't bring too much overhead when retry.
	if t.DialRetryFrequency == 0 {
		t.DialRetryFrequency = rate.Every(100 * time.Millisecond)
	}
	return nil
}

// 创建消息通道并注册
func (t *Transport) Handler() http.Handler {
	// 创建 pipelineHandler、streamHandler 和 snapshotHandler 三个实例，这三个实例都实现了 http.Server.Handler 接口
	pipelineHandler := newPipelineHandler(t, t.Raft, t.ClusterID)
	streamHandler := newStreamHandler(t, t, t.Raft, t.ID, t.ClusterID)
	snapHandler := newSnapshotHandler(t, t.Raft, t.Snapshotter, t.ClusterID)
	// 创建 ServeMux 实例，ServeMux 是一个多路复用器，ServeMux 主要通过其 m 字段 (map[string)muxEntry 类型）存储具体的 URL 和 Handler 实例之间的映射关系
	mux := http.NewServeMux()
	// 设置 URL 与 Handler 实例的映射关系，也就是访问指定 URL 地址的请求，由对应的 Handler 实例进行处理
	mux.Handle(RaftPrefix, pipelineHandler)         // "/raft"			-->	pipelineHandler{}
	mux.Handle(RaftStreamPrefix+"/", streamHandler) // "/raft/stream/"	-->	streamHandler{}
	mux.Handle(RaftSnapshotPrefix, snapHandler)     // "/raft/snapshot"	-->	snapHandler{}
	mux.Handle(ProbingPrefix, probing.NewHandler()) // "/raft/probing"	-->	httpHealth{}
	return mux
}

func (t *Transport) Get(id types.ID) Peer {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.peers[id]
}

func (t *Transport) Send(msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.To == 0 {
			// 忽略故意丢弃的信息
			continue
		}
		to := types.ID(m.To) // 获取目标节点

		t.mu.RLock()
		p, pok := t.peers[to]
		g, rok := t.remotes[to]
		t.mu.RUnlock()

		if pok { // 如果存在对应的 Peer 实例，则使用 Peer 实例发送消息
			if m.Type == raftpb.MsgApp { // 统计信息
				t.ServerStats.SendAppendReq(m.Size())
			}
			p.send(m)
			continue
		}

		if rok { // 如采指定节点 ID 不存在对应的 Peer 实例，则尝试使用查找对应 remote 实例
			g.send(m)
			continue
		}

		// 无法找到目标节点，打印 debug 日志。继续下一次循环
		if t.Logger != nil {
			t.Logger.Debug(
				"ignored message send request; unknown remote peer target",
				zap.String("type", m.Type.String()),
				zap.String("unknown-target-peer-id", to.String()),
			)
		} else {
			plog.Debugf("ignored message %s (sent to unknown peer %s)", m.Type, to)
		}
	}
}

func (t *Transport) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, r := range t.remotes {
		r.stop()
	}
	for _, p := range t.peers {
		p.stop()
	}
	t.pipelineProber.RemoveAll()
	t.streamProber.RemoveAll()
	if tr, ok := t.streamRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	if tr, ok := t.pipelineRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	t.peers = nil
	t.remotes = nil
}

// CutPeer drops messages to the specified peer.
func (t *Transport) CutPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	g, gok := t.remotes[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Pause()
	}
	if gok {
		g.Pause()
	}
}

// MendPeer recovers the message dropping behavior of the given peer.
func (t *Transport) MendPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	g, gok := t.remotes[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Resume()
	}
	if gok {
		g.Resume()
	}
}

func (t *Transport) AddRemote(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.remotes == nil {
		// there's no clean way to shutdown the golang http server
		// (see: https://github.com/golang/go/issues/4674) before
		// stopping the transport; ignore any new connections.
		return
	}
	if _, ok := t.peers[id]; ok {
		return
	}
	if _, ok := t.remotes[id]; ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
		if t.Logger != nil {
			t.Logger.Panic("failed NewURLs", zap.Strings("urls", us), zap.Error(err))
		} else {
			plog.Panicf("newURLs %+v should never fail: %+v", us, err)
		}
	}
	t.remotes[id] = startRemote(t, urls, id)

	if t.Logger != nil {
		t.Logger.Info(
			"added new remote peer",
			zap.String("local-member-id", t.ID.String()),
			zap.String("remote-peer-id", id.String()),
			zap.Strings("remote-peer-urls", us),
		)
	}
}

// 创建并启用对应节点的 Peer 实例
func (t *Transport) AddPeer(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.peers == nil {
		panic("transport stopped")
	}
	if _, ok := t.peers[id]; ok {
		return
	}
	urls, err := types.NewURLs(us) // 解析 us 切片中指定的 url 链接
	if err != nil {
		if t.Logger != nil {
			t.Logger.Panic("failed NewURLs", zap.Strings("urls", us), zap.Error(err))
		} else {
			plog.Panicf("newURLs %+v should never fail: %+v", us, err)
		}
	}
	fs := t.LeaderStats.Follower(id.String())
	t.peers[id] = startPeer(t, urls, id, fs) // 创建指定节点对应的 Peer 实例，其中会创建相关的 pipeline 和 stream 消息通道
	// 每隔一段时间，探针发送探测信息，检测对端健康状态
	addPeerToProber(t.Logger, t.pipelineProber, id.String(), us, RoundTripperNameSnapshot, rttSec)
	addPeerToProber(t.Logger, t.streamProber, id.String(), us, RoundTripperNameRaftMessage, rttSec)

	if t.Logger != nil {
		t.Logger.Info(
			"added remote peer",
			zap.String("local-member-id", t.ID.String()),
			zap.String("remote-peer-id", id.String()),
			zap.Strings("remote-peer-urls", us),
		)
	} else {
		plog.Infof("added peer %s", id)
	}
}

func (t *Transport) RemovePeer(id types.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removePeer(id)
}

func (t *Transport) RemoveAllPeers() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id := range t.peers {
		t.removePeer(id)
	}
}

// the caller of this function must have the peers mutex.
func (t *Transport) removePeer(id types.ID) {
	if peer, ok := t.peers[id]; ok {
		peer.stop() // 关闭 peer 实例
	} else {
		if t.Logger != nil {
			t.Logger.Panic("unexpected removal of unknown remote peer", zap.String("remote-peer-id", id.String()))
		} else {
			plog.Panicf("unexpected removal of unknown peer '%d'", id)
		}
	}
	delete(t.peers, id)
	delete(t.LeaderStats.Followers, id.String())
	t.pipelineProber.Remove(id.String())
	t.streamProber.Remove(id.String())

	if t.Logger != nil {
		t.Logger.Info(
			"removed remote peer",
			zap.String("local-member-id", t.ID.String()),
			zap.String("removed-remote-peer-id", id.String()),
		)
	} else {
		plog.Infof("removed peer %s", id)
	}
}

func (t *Transport) UpdatePeer(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// TODO: return error or just panic?
	if _, ok := t.peers[id]; !ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
		if t.Logger != nil {
			t.Logger.Panic("failed NewURLs", zap.Strings("urls", us), zap.Error(err))
		} else {
			plog.Panicf("newURLs %+v should never fail: %+v", us, err)
		}
	}
	t.peers[id].update(urls)

	t.pipelineProber.Remove(id.String())
	addPeerToProber(t.Logger, t.pipelineProber, id.String(), us, RoundTripperNameSnapshot, rttSec)
	t.streamProber.Remove(id.String())
	addPeerToProber(t.Logger, t.streamProber, id.String(), us, RoundTripperNameRaftMessage, rttSec)

	if t.Logger != nil {
		t.Logger.Info(
			"updated remote peer",
			zap.String("local-member-id", t.ID.String()),
			zap.String("updated-remote-peer-id", id.String()),
			zap.Strings("updated-remote-peer-urls", us),
		)
	} else {
		plog.Infof("updated peer %s", id)
	}
}

func (t *Transport) ActiveSince(id types.ID) time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if p, ok := t.peers[id]; ok {
		return p.activeSince()
	}
	return time.Time{}
}

func (t *Transport) SendSnapshot(m snap.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.peers[types.ID(m.To)]
	if p == nil {
		m.CloseWithError(errMemberNotFound)
		return
	}
	p.sendSnap(m)
}

// Pausable is a testing interface for pausing transport traffic.
type Pausable interface {
	Pause()
	Resume()
}

func (t *Transport) Pause() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		p.(Pausable).Pause()
	}
}

func (t *Transport) Resume() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		p.(Pausable).Resume()
	}
}

// ActivePeers returns a channel that closes when an initial
// peer connection has been established. Use this to wait until the
// first peer connection becomes active.
func (t *Transport) ActivePeers() (cnt int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		if !p.activeSince().IsZero() {
			cnt++
		}
	}
	return cnt
}
