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

// Inflights limits the number of MsgApp (represented by the largest index
// contained within) sent to followers but not yet acknowledged by them. Callers
// use Full() to check whether more messages can be sent, call Add() whenever
// they are sending a new append, and release "quota" via FreeLE() whenever an
// ack is received.
type Inflights struct {
	// inflights.buffer 数组被当作一个环形数组使用， start 字段中记录 buffer 中第一条 MsgApp 消息的下标
	start int
	// 当前 inflights 实例中记录的 MsgApp 消息个数
	count int

	// buffer 大小;当前 inflights 实例中能够记录的 MsgApp 消息个数的上限
	size int

	// 用来记录 MsgApp 消息相关信息的数组，其中记录的是 MsgApp 消息中最后一条 Entry 记录的索引值
	buffer []uint64
}

// NewInflights sets up an Inflights that allows up to 'size' inflight messages.
func NewInflights(size int) *Inflights {
	return &Inflights{
		size: size,
	}
}

// Clone returns an *Inflights that is identical to but shares no memory with
// the receiver.
func (in *Inflights) Clone() *Inflights {
	ins := *in
	ins.buffer = append([]uint64(nil), in.buffer...)
	return &ins
}

// Add notifies the Inflights that a new message with the given index is being
// dispatched. Full() must be called prior to Add() to verify that there is room
// for one more message, and consecutive calls to add Add() must provide a
// monotonic sequence of indexes.
func (in *Inflights) Add(inflight uint64) {
	if in.Full() { // 检测 buffer 数组是否已满
		panic("cannot add into a Full inflights")
	}
	next := in.start + in.count // 获取新增消息的下标
	size := in.size             // 记录消息的上限，等同于数组最大长度
	if next >= size {           // 环形队列
		next -= size
	}
	// 扩容 buffer 数组
	if next >= len(in.buffer) {
		in.grow()
	}
	in.buffer[next] = inflight // 记录 inflight 在 next 位置
	in.count++                 // count 递增
}

// grow the inflight buffer by doubling up to inflights.size. We grow on demand
// instead of preallocating to inflights.size to handle systems which have
// thousands of Raft groups per process.
func (in *Inflights) grow() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	} else if newSize > in.size {
		newSize = in.size
	}
	newBuffer := make([]uint64, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}

// FreeLE frees the inflights smaller or equal to the given `to` flight.
func (in *Inflights) FreeLE(to uint64) {
	if in.count == 0 || to < in.buffer[in.start] {
		// out of the left side of the window
		return
	}

	idx := in.start
	var i int
	for i = 0; i < in.count; i++ {
		if to < in.buffer[idx] { // 查找第一个大于指定索引 to 的 inflight
			break
		}

		// increase index and maybe rotate
		// 因为是环形队列，如果 idx 越界（超过 size），则需要重新转换 buffer 数组下标
		size := in.size
		if idx++; idx >= size {
			idx -= size
		}
	}
	// free i inflights and set new start index
	in.count -= i      // i 记录释放的消息个数
	in.start = idx     // 环形队列从 start 到 idx 间的数据被释放（注意实际数据值为释放，但是初始下标 start 和 count 被修改，数组数据等同于被标记失效）
	if in.count == 0 { // 如果 inflight 全部被清空
		// inflights is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.
		in.start = 0 // 则重置 start，等同于重置 buffer
	}
}

// FreeFirstOne releases the first inflight. This is a no-op if nothing is
// inflight.
func (in *Inflights) FreeFirstOne() { in.FreeLE(in.buffer[in.start]) }

// Full returns true if no more messages can be sent at the moment.
func (in *Inflights) Full() bool {
	return in.count == in.size
}

// Count returns the number of inflight messages.
func (in *Inflights) Count() int { return in.count }

// reset frees all inflights.
func (in *Inflights) reset() {
	in.count = 0
	in.start = 0
}
