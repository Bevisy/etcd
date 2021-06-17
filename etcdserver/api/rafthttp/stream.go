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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/httputil"
	"go.etcd.io/etcd/pkg/transport"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/version"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	streamTypeMessage  streamType = "message"
	streamTypeMsgAppV2 streamType = "msgappv2"

	streamBufSize = 4096
)

var (
	errUnsupportedStreamType = fmt.Errorf("unsupported stream type")

	// the key is in string format "major.minor.patch"
	supportedStream = map[string][]streamType{
		"2.0.0": {},
		"2.1.0": {streamTypeMsgAppV2, streamTypeMessage},
		"2.2.0": {streamTypeMsgAppV2, streamTypeMessage},
		"2.3.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.0.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.1.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.2.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.3.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.4.0": {streamTypeMsgAppV2, streamTypeMessage},
	}
)

type streamType string

func (t streamType) endpoint() string {
	switch t {
	case streamTypeMsgAppV2:
		return path.Join(RaftStreamPrefix, "msgapp")
	case streamTypeMessage:
		return path.Join(RaftStreamPrefix, "message")
	default:
		plog.Panicf("unhandled stream type %v", t)
		return ""
	}
}

func (t streamType) String() string {
	switch t {
	case streamTypeMsgAppV2:
		return "stream MsgApp v2"
	case streamTypeMessage:
		return "stream Message"
	default:
		return "unknown stream"
	}
}

var (
	// linkHeartbeatMessage is a special message used as heartbeat message in
	// link layer. It never conflicts with messages from raft because raft
	// doesn't send out messages without From and To fields.
	linkHeartbeatMessage = raftpb.Message{Type: raftpb.MsgHeartbeat}
)

func isLinkHeartbeatMessage(m *raftpb.Message) bool {
	return m.Type == raftpb.MsgHeartbeat && m.From == 0 && m.To == 0
}

type outgoingConn struct {
	t streamType
	io.Writer
	http.Flusher
	io.Closer

	localID types.ID
	peerID  types.ID
}

// streamWriter writes messages to the attached outgoingConn.
type streamWriter struct {
	lg *zap.Logger

	localID types.ID // 本地节点 ID
	peerID  types.ID // 对端节点 ID

	status *peerStatus // peer 节点状态
	fs     *stats.FollowerStats
	r      Raft // 底层 Raft 实例

	mu      sync.Mutex // guard field working and closer
	closer  io.Closer  // 负责关闭底层长连接
	working bool       // 负责标识当前的 streamWriter 是否可用（底层是否关联了相应的网络连接）

	msgc  chan raftpb.Message // 待发送的消息写入该通道
	connc chan *outgoingConn  // 通过该通道获取当前 streamWriter 实例关联的底层网络连接， outgoingConn 其实是对网络连接的一层封装，其中记录了当前连接使用	的协议版本，以及用于关闭连接的 Flusher 和 Closer 等信息
	stopc chan struct{}
	done  chan struct{}
}

// startStreamWriter creates a streamWrite and starts a long running go-routine that accepts
// messages and writes to the attached outgoing connection.
//
// startSreamWriter 创建一个 streamWriter 实例并且启动一个长期运行的协程用来接收消息并写入关联的出口网络连接
func startStreamWriter(lg *zap.Logger, local, id types.ID, status *peerStatus, fs *stats.FollowerStats, r Raft) *streamWriter {
	w := &streamWriter{
		lg: lg,

		localID: local,
		peerID:  id,

		status: status,
		fs:     fs,
		r:      r,
		msgc:   make(chan raftpb.Message, streamBufSize),
		connc:  make(chan *outgoingConn),
		stopc:  make(chan struct{}),
		done:   make(chan struct{}),
	}
	go w.run()
	return w
}

// 主要作用：
// 1. 当其它节点主动与当前节点创建连接（即 Stream 消息通道底层使用的网络连接〉时，该连接实例会写入对应 peer.writer.connc 通道， 在 streamWriter.run()方法中会通过该通道获取该连接实例井进行绑定，之后才能开始后续的消息发送
// 2. 定时发送心跳消息，该心跳消息并不是前面介绍 etcd-raft 模块时提到的 MsgHeartbeat消息，而是为了防止底层连接超时的消息
// 3. 发送除心跳消息外的其他类型的消息
func (cw *streamWriter) run() {
	var (
		msgc       chan raftpb.Message // 指向当前 streamWriter.msgc 字段
		heartbeatc <-chan time.Time    // 定时器会定时向该通道发送信号，触发心跳消息的发送，该心跳消息与后面介绍的 Raft 的心跳消息有所不同，该心跳消息的主要目的是为了防止连接长时间不用断开
		t          streamType          // 记录消息的版本信息
		enc        encoder             // 编码器，负责将消息序列化并写入连接的缓冲区
		flusher    http.Flusher        // 负责刷新底层连接，将数据真正发送出去
		batched    int                 // 当前未 flush 的消息个数
	)
	tickc := time.NewTicker(ConnReadTimeout / 3) // 发送心跳消息的定时器
	defer tickc.Stop()
	unflushed := 0 // 未 Flush 的字节数

	if cw.lg != nil {
		cw.lg.Info(
			"started stream writer with remote peer",
			zap.String("local-member-id", cw.localID.String()),
			zap.String("remote-peer-id", cw.peerID.String()),
		)
	} else {
		plog.Infof("started streaming with peer %s (writer)", cw.peerID)
	}

	for {
		select {
		case <-heartbeatc: // 定时器到期，触发心跳消息
			err := enc.encode(&linkHeartbeatMessage) // 通过 encoder 发送心跳消息
			unflushed += linkHeartbeatMessage.Size() // 增加未 Flush 出去的字节数
			// 若没有异常，则使用 flusher 将缓存的消息全部发送出去，并重置 batched 和 unflushed 两个统计变量
			if err == nil {
				flusher.Flush()
				batched = 0
				sentBytes.WithLabelValues(cw.peerID.String()).Add(float64(unflushed))
				unflushed = 0
				continue
			}

			cw.status.deactivate(failureType{source: t.String(), action: "heartbeat"}, err.Error())

			sentFailures.WithLabelValues(cw.peerID.String()).Inc()
			cw.close() // 发生异常，则关闭 streamWriter，会导致底层连接关闭
			if cw.lg != nil {
				cw.lg.Warn(
					"lost TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("local-member-id", cw.localID.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			} else {
				plog.Warningf("lost the TCP streaming connection with peer %s (%s writer)", cw.peerID, t)
			}
			// 将 heartbeatc 和 msgc 两个通道清空，后续就不会再发送心跳消息和其他类型的消息
			heartbeatc, msgc = nil, nil

		case m := <-msgc: // peer 向 streamWriter.msgc 写入待发送的消息
			err := enc.encode(&m) // 将消息序列化，并写入底层连接
			if err == nil {
				unflushed += m.Size() // 如果无异常，则递增 unflushed 变量

				// msgc 通道中的消息全部发送完成或是未 Flush 的消息较多，则触发 Flush，否则只是递增 batched 变量
				if len(msgc) == 0 || batched > streamBufSize/2 {
					flusher.Flush()
					sentBytes.WithLabelValues(cw.peerID.String()).Add(float64(unflushed))
					unflushed = 0
					batched = 0
				} else {
					batched++
				}

				continue
			}

			// 异常处理
			cw.status.deactivate(failureType{source: t.String(), action: "write"}, err.Error())
			cw.close()
			if cw.lg != nil {
				cw.lg.Warn(
					"lost TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("local-member-id", cw.localID.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			} else {
				plog.Warningf("lost the TCP streaming connection with peer %s (%s writer)", cw.peerID, t)
			}
			heartbeatc, msgc = nil, nil
			cw.r.ReportUnreachable(m.To)
			sentFailures.WithLabelValues(cw.peerID.String()).Inc()

		// 当其他节点主动与当前节点创建 Stream 消息通道时，会先通过 StreamHandler 的处理，StreamHandler 会通过 attach() 方法将连接写入对应 peer.writer.connc 通道，而当前的 goroutine 会通过该通道获取连接，然后开始发送消息
		case conn := <-cw.connc: // 获取与当前 streamWriter 实例绑定的底层连接
			cw.mu.Lock()
			closed := cw.closeUnlocked()
			t = conn.t // 获取该连接底层发送的消息版本，并创建相应的 encoder 实例
			switch conn.t {
			case streamTypeMsgAppV2:
				enc = newMsgAppV2Encoder(conn.Writer, cw.fs)
			case streamTypeMessage:
				// 将 http.ResponseWriter 封装成 messageEncoder，上层调用通过 messageEncoder 实例完成消息发送
				enc = &messageEncoder{w: conn.Writer}
			default:
				plog.Panicf("unhandled stream type %s", conn.t)
			}
			if cw.lg != nil {
				cw.lg.Info(
					"set message encoder",
					zap.String("from", conn.localID.String()),
					zap.String("to", conn.peerID.String()),
					zap.String("stream-type", t.String()),
				)
			}
			flusher = conn.Flusher  // 记录底层连接对应的 Flusher
			unflushed = 0           // 重置未 Flush 的字节数
			cw.status.activate()    // peerStatus.activeit直为 true
			cw.closer = conn.Closer // 记录底层连接对应的 Closer
			cw.working = true       // 标识当前 streamWriter 正在运行
			cw.mu.Unlock()

			if closed {
				if cw.lg != nil {
					cw.lg.Warn(
						"closed TCP streaming connection with remote peer",
						zap.String("stream-writer-type", t.String()),
						zap.String("local-member-id", cw.localID.String()),
						zap.String("remote-peer-id", cw.peerID.String()),
					)
				} else {
					plog.Warningf("closed an existing TCP streaming connection with peer %s (%s writer)", cw.peerID, t)
				}
			}
			if cw.lg != nil {
				cw.lg.Warn(
					"established TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("local-member-id", cw.localID.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			} else {
				plog.Infof("established a TCP streaming connection with peer %s (%s writer)", cw.peerID, t)
			}
			// 更新 heartbeatc 和 msgc 两个通道，此后可以发送消息
			heartbeatc, msgc = tickc.C, cw.msgc

		case <-cw.stopc:
			if cw.close() {
				if cw.lg != nil {
					cw.lg.Warn(
						"closed TCP streaming connection with remote peer",
						zap.String("stream-writer-type", t.String()),
						zap.String("remote-peer-id", cw.peerID.String()),
					)
				} else {
					plog.Infof("closed the TCP streaming connection with peer %s (%s writer)", cw.peerID, t)
				}
			}
			if cw.lg != nil {
				cw.lg.Warn(
					"stopped TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			} else {
				plog.Infof("stopped streaming with peer %s (writer)", cw.peerID)
			}
			close(cw.done)
			return
		}
	}
}

func (cw *streamWriter) writec() (chan<- raftpb.Message, bool) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.msgc, cw.working
}

func (cw *streamWriter) close() bool {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.closeUnlocked()
}

func (cw *streamWriter) closeUnlocked() bool {
	if !cw.working {
		return false
	}
	if err := cw.closer.Close(); err != nil {
		if cw.lg != nil {
			cw.lg.Warn(
				"failed to close connection with remote peer",
				zap.String("remote-peer-id", cw.peerID.String()),
				zap.Error(err),
			)
		} else {
			plog.Errorf("peer %s (writer) connection close error: %v", cw.peerID, err)
		}
	}
	if len(cw.msgc) > 0 {
		cw.r.ReportUnreachable(uint64(cw.peerID))
	}
	cw.msgc = make(chan raftpb.Message, streamBufSize)
	cw.working = false
	return true
}

func (cw *streamWriter) attach(conn *outgoingConn) bool {
	select {
	case cw.connc <- conn: // 将 conn 实例写入 streamWriter.connc 通道中
		return true
	case <-cw.done:
		return false
	}
}

func (cw *streamWriter) stop() {
	close(cw.stopc)
	<-cw.done
}

// streamReader is a long-running go-routine that dials to the remote stream
// endpoint and reads messages from the response body returned.
//
// streamReader 是一个长期运行的协程，用来拨号远端流端点，并且从返回的消息体读取信息
type streamReader struct {
	lg *zap.Logger

	peerID types.ID   // 对端节点 ID
	typ    streamType // 关联底层联结使用的协议版本信息

	tr     *Transport            // 关联 rafthttp.Transport 实例
	picker *urlPicker            // 获取对端节点可用的 URL
	status *peerStatus           // 对端节点状态
	recvc  chan<- raftpb.Message // 从对端节点发送来的非 MsgProp 类型的消息会首先由 streamReader 写入 recvc 通道中，然后由 peer.start()启动的后台 goroutine 读取出来，交由底层的 eted-raft 模块进行处理
	propc  chan<- raftpb.Message // 该通道与 recvc 通道类似，只不过其中接收的是 MsgProp 类型的消息

	rl *rate.Limiter // alters the frequency of dial retrial attempts

	errorc chan<- error

	mu     sync.Mutex
	paused bool      // 是否暂停读取数据
	closer io.Closer // 负责关闭底层的长连接

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (cr *streamReader) start() {
	cr.done = make(chan struct{})
	if cr.errorc == nil {
		cr.errorc = cr.tr.ErrorC
	}
	if cr.ctx == nil {
		cr.ctx, cr.cancel = context.WithCancel(context.Background())
	}
	go cr.run()
}

func (cr *streamReader) run() {
	t := cr.typ // 获取使用的消息版本

	if cr.lg != nil {
		cr.lg.Info(
			"started stream reader with remote peer",
			zap.String("stream-reader-type", t.String()),
			zap.String("local-member-id", cr.tr.ID.String()),
			zap.String("remote-peer-id", cr.peerID.String()),
		)
	} else {
		plog.Infof("started streaming with peer %s (%s reader)", cr.peerID, t)
	}

	for {
		// 向对端节点发送一个 GET 请求，然后获取并返回相应的 ReadCloser
		rc, err := cr.dial(t)
		if err != nil {
			if err != errUnsupportedStreamType {
				cr.status.deactivate(failureType{source: t.String(), action: "dial"}, err.Error())
			}
		} else {
			cr.status.activate()
			if cr.lg != nil {
				cr.lg.Info(
					"established TCP streaming connection with remote peer",
					zap.String("stream-reader-type", cr.typ.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
				)
			} else {
				plog.Infof("established a TCP streaming connection with peer %s (%s reader)", cr.peerID, cr.typ)
			}
			// 如果未出现异常，则开始读取对端返回的消息，并将读取到的消息写入 streamReader.recvc 通道中
			err = cr.decodeLoop(rc, t)
			if cr.lg != nil {
				cr.lg.Warn(
					"lost TCP streaming connection with remote peer",
					zap.String("stream-reader-type", cr.typ.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(err),
				)
			} else {
				plog.Warningf("lost the TCP streaming connection with peer %s (%s reader)", cr.peerID, cr.typ)
			}
			switch {
			// all data is read out
			case err == io.EOF:
			// connection is closed by the remote
			case transport.IsClosedConnError(err):
			default:
				cr.status.deactivate(failureType{source: t.String(), action: "read"}, err.Error())
			}
		}
		// Wait for a while before new dial attempt
		err = cr.rl.Wait(cr.ctx)
		if cr.ctx.Err() != nil {
			if cr.lg != nil {
				cr.lg.Info(
					"stopped stream reader with remote peer",
					zap.String("stream-reader-type", t.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
				)
			} else {
				plog.Infof("stopped streaming with peer %s (%s reader)", cr.peerID, t)
			}
			close(cr.done)
			return
		}
		if err != nil {
			if cr.lg != nil {
				cr.lg.Warn(
					"rate limit on stream reader with remote peer",
					zap.String("stream-reader-type", t.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(err),
				)
			} else {
				plog.Errorf("streaming with peer %s (%s reader) rate limiter error: %v", cr.peerID, t, err)
			}
		}
	}
}

// streamReader 中的核心方法，它会从底层的网络连接读取数据并进行反序列化，之后将得到的消息实例写入 recvc 通道（或 propc 通道）中，等待 Peer 进行处理
func (cr *streamReader) decodeLoop(rc io.ReadCloser, t streamType) error {
	var dec decoder
	cr.mu.Lock()
	// 根据使用的消息协议版本创建对应的 decoder 实例
	switch t {
	case streamTypeMsgAppV2:
		dec = newMsgAppV2Decoder(rc, cr.tr.ID, cr.peerID)
	case streamTypeMessage:
		dec = &messageDecoder{r: rc} // messageDecoder 主要负责从连接中读取数据
	default:
		if cr.lg != nil {
			cr.lg.Panic("unknown stream type", zap.String("type", t.String()))
		} else {
			plog.Panicf("unhandled stream type %s", t)
		}
	}
	// 检测 streamReader 是否关闭，若关闭则返回异常
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		if err := rc.Close(); err != nil {
			return err
		}
		return io.EOF
	default:
		cr.closer = rc
	}
	cr.mu.Unlock()

	// gofail: labelRaftDropHeartbeat:
	for {
		// 从底层连接中读取数据，并反序列化成 raftpb.Message 实例
		m, err := dec.decode()
		if err != nil {
			cr.mu.Lock()
			cr.close()
			cr.mu.Unlock()
			return err
		}

		// gofail-go: var raftDropHeartbeat struct{}
		// continue labelRaftDropHeartbeat
		receivedBytes.WithLabelValues(types.ID(m.From).String()).Add(float64(m.Size()))

		cr.mu.Lock()
		paused := cr.paused
		cr.mu.Unlock()

		if paused { // 检测是否已经暂停从该连接上读取消息
			continue
		}

		if isLinkHeartbeatMessage(&m) { // 忽略连接层的心跳消息，注意与 Raft 的心跳消息进行区分。连接层心跳信息没有 m.to
			// raft is not interested in link layer
			// heartbeat message, so we should ignore
			// it.
			continue
		}

		// 根据读取到的消息类型，选择对应通道进行写入
		recvc := cr.recvc
		if m.Type == raftpb.MsgProp {
			recvc = cr.propc
		}

		select {
		case recvc <- m: // 将消息写入对应的通道中，之后会交给底层的 Raft 状态机进行处理
		default: // recvc 通道满了之后，会丢弃消息，并打印日志
			if cr.status.isActive() {
				if cr.lg != nil {
					cr.lg.Warn(
						"dropped internal Raft message since receiving buffer is full (overloaded network)",
						zap.String("message-type", m.Type.String()),
						zap.String("local-member-id", cr.tr.ID.String()),
						zap.String("from", types.ID(m.From).String()),
						zap.String("remote-peer-id", types.ID(m.To).String()),
						zap.Bool("remote-peer-active", cr.status.isActive()),
					)
				} else {
					plog.MergeWarningf("dropped internal raft message from %s since receiving buffer is full (overloaded network)", types.ID(m.From))
				}
			} else {
				if cr.lg != nil {
					cr.lg.Warn(
						"dropped Raft message since receiving buffer is full (overloaded network)",
						zap.String("message-type", m.Type.String()),
						zap.String("local-member-id", cr.tr.ID.String()),
						zap.String("from", types.ID(m.From).String()),
						zap.String("remote-peer-id", types.ID(m.To).String()),
						zap.Bool("remote-peer-active", cr.status.isActive()),
					)
				} else {
					plog.Debugf("dropped %s from %s since receiving buffer is full", m.Type, types.ID(m.From))
				}
			}
			recvFailures.WithLabelValues(types.ID(m.From).String()).Inc()
		}
	}
}

func (cr *streamReader) stop() {
	cr.mu.Lock()
	cr.cancel()
	cr.close()
	cr.mu.Unlock()
	<-cr.done
}

// 与对端节点建立连接，请求成功，则返会响应体
func (cr *streamReader) dial(t streamType) (io.ReadCloser, error) {
	u := cr.picker.pick() // 获取对端节点可用的 URL 地址
	uu := u
	// 根据使用协议的版本和节点 ID 创建最终的 URL 地址
	uu.Path = path.Join(t.endpoint(), cr.tr.ID.String())

	if cr.lg != nil {
		cr.lg.Debug(
			"dial stream reader",
			zap.String("from", cr.tr.ID.String()),
			zap.String("to", cr.peerID.String()),
			zap.String("address", uu.String()),
		)
	}
	// 创建 GET 请求
	req, err := http.NewRequest("GET", uu.String(), nil)
	if err != nil {
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("failed to make http request to %v (%v)", u, err)
	}
	// 设置 HTTP 请求头
	req.Header.Set("X-Server-From", cr.tr.ID.String())
	req.Header.Set("X-Server-Version", version.Version)
	req.Header.Set("X-Min-Cluster-Version", version.MinClusterVersion)
	req.Header.Set("X-Etcd-Cluster-ID", cr.tr.ClusterID.String())
	req.Header.Set("X-Raft-To", cr.peerID.String())

	// 将当前节点暴露的 URL 一起发送给对端节点
	setPeerURLsHeader(req, cr.tr.URLs)

	req = req.WithContext(cr.ctx)

	cr.mu.Lock()
	// 检测当前 streamReader 是否被关闭，若已被关闭，则返回异常
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		return nil, fmt.Errorf("stream reader is stopped")
	default:
	}
	cr.mu.Unlock()

	// 通过 rafthttp.Transport.streamRt 与对端建立连接
	resp, err := cr.tr.streamRt.RoundTrip(req)
	if err != nil {
		cr.picker.unreachable(u)
		return nil, err
	}

	// 解析 HTTP 响应，检测版本信息
	rv := serverVersion(resp.Header)
	lv := semver.Must(semver.NewVersion(version.Version))
	if compareMajorMinorVersion(rv, lv) == -1 && !checkStreamSupport(rv, t) {
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, errUnsupportedStreamType
	}

	// 根据 HTTP 的响应码进行处理
	switch resp.StatusCode {
	case http.StatusGone:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		reportCriticalError(errMemberRemoved, cr.errorc)
		return nil, errMemberRemoved

	case http.StatusOK: // 请求正常，返回响应体
		return resp.Body, nil

	case http.StatusNotFound:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("peer %s failed to find local node %s", cr.peerID, cr.tr.ID)

	case http.StatusPreconditionFailed:
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			cr.picker.unreachable(u)
			return nil, err
		}
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)

		switch strings.TrimSuffix(string(b), "\n") {
		case errIncompatibleVersion.Error():
			if cr.lg != nil {
				cr.lg.Warn(
					"request sent was ignored by remote peer due to server version incompatibility",
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(errIncompatibleVersion),
				)
			} else {
				plog.Errorf("request sent was ignored by peer %s (server version incompatible)", cr.peerID)
			}
			return nil, errIncompatibleVersion

		case errClusterIDMismatch.Error():
			if cr.lg != nil {
				cr.lg.Warn(
					"request sent was ignored by remote peer due to cluster ID mismatch",
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.String("remote-peer-cluster-id", resp.Header.Get("X-Etcd-Cluster-ID")),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("local-member-cluster-id", cr.tr.ClusterID.String()),
					zap.Error(errClusterIDMismatch),
				)
			} else {
				plog.Errorf("request sent was ignored (cluster ID mismatch: peer[%s]=%s, local=%s)",
					cr.peerID, resp.Header.Get("X-Etcd-Cluster-ID"), cr.tr.ClusterID)
			}
			return nil, errClusterIDMismatch

		default:
			return nil, fmt.Errorf("unhandled error %q when precondition failed", string(b))
		}

	default:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("unhandled http status %d", resp.StatusCode)
	}
}

func (cr *streamReader) close() {
	if cr.closer != nil {
		if err := cr.closer.Close(); err != nil {
			if cr.lg != nil {
				cr.lg.Warn(
					"failed to close remote peer connection",
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(err),
				)
			} else {
				plog.Errorf("peer %s (reader) connection close error: %v", cr.peerID, err)
			}
		}
	}
	cr.closer = nil
}

func (cr *streamReader) pause() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = true
}

func (cr *streamReader) resume() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = false
}

// checkStreamSupport checks whether the stream type is supported in the
// given version.
func checkStreamSupport(v *semver.Version, t streamType) bool {
	nv := &semver.Version{Major: v.Major, Minor: v.Minor}
	for _, s := range supportedStream[nv.String()] {
		if s == t {
			return true
		}
	}
	return false
}
