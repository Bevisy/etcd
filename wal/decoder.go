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

package wal

import (
	"bufio"
	"encoding/binary"
	"hash"
	"io"
	"sync"

	"go.etcd.io/etcd/pkg/crc"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal/walpb"
)

const minSectorSize = 512

// frameSizeBytes is frame size in bytes, including record size and padding size.
const frameSizeBytes = 8

type decoder struct {
	mu  sync.Mutex
	brs []*bufio.Reader // 该 decoder 实例通过该字段中记录的 Reader 实例读取相应的日志文件，这些日志文件就是在 wal.openAtlndex()方法中打开的日志文件

	// lastValidOff file offset following the last valid decoded record
	//
	// 读取日志记录的指针
	lastValidOff int64
	crc          hash.Hash32
}

func newDecoder(r ...io.Reader) *decoder {
	readers := make([]*bufio.Reader, len(r))
	for i := range r {
		readers[i] = bufio.NewReader(r[i])
	}
	return &decoder{
		brs: readers,
		crc: crc.New(0, crcTable),
	}
}

func (d *decoder) decode(rec *walpb.Record) error {
	rec.Reset()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.decodeRecord(rec)
}

// raft max message size is set to 1 MB in etcd server
// assume projects set reasonable message size limit,
// thus entry size should never exceed 10 MB
const maxWALEntrySizeLimit = int64(10 * 1024 * 1024)

func (d *decoder) decodeRecord(rec *walpb.Record) error {
	// 检测 brs 字段长度，决定是否有日志文件需要读取
	if len(d.brs) == 0 {
		return io.EOF
	}

	// 读取第一个文件中第一个日志记录的长度
	l, err := readInt64(d.brs[0])
	// 是否读取到文件尾或者读取到与分配的部分，均表示读取操作结束
	if err == io.EOF || (err == nil && l == 0) {
		// hit end of file or preallocated space
		// 更新 brs 字段，将其中第一个日志文件对应的 Reader 清除掉
		d.brs = d.brs[1:]
		// 如果后面没有其它日志可读则返回 io.EOF 异常，表示读取正常结束
		if len(d.brs) == 0 {
			return io.EOF
		}
		// 若后续还有其它文件需要读取，则需要切换文件，此时会重置 lastValidOff
		d.lastValidOff = 0
		// 递归调用 decodeRecord() 方法
		return d.decodeRecord(rec)
	}
	if err != nil {
		return err
	}

	// 计算当前日志记录的实际长度及填充数据的长度，并创建相应的 data 切片
	recBytes, padBytes := decodeFrameSize(l)
	if recBytes >= maxWALEntrySizeLimit-padBytes {
		return ErrMaxWALEntrySizeLimitExceeded
	}

	// 从日志文件中读取指定长度的字节数，如果读取不到指定的字节数，则返回 EOF 异常
	// 返回 ErrUnexpectedEOF 异常
	data := make([]byte, recBytes+padBytes)
	if _, err = io.ReadFull(d.brs[0], data); err != nil {
		// ReadFull returns io.EOF only if no bytes were read
		// the decoder should treat this as an ErrUnexpectedEOF instead.
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	// 反序列化
	if err := rec.Unmarshal(data[:recBytes]); err != nil {
		if d.isTornEntry(data) {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	// 如果记录类型为 crcType，则跳过 crc 校验
	if rec.Type != crcType {
		d.crc.Write(rec.Data)
		if err := rec.Validate(d.crc.Sum32()); err != nil {
			if d.isTornEntry(data) {
				return io.ErrUnexpectedEOF
			}
			return err
		}
	}
	// record decoded as valid; point last valid offset to end of record
	d.lastValidOff += frameSizeBytes + recBytes + padBytes
	return nil
}

func decodeFrameSize(lenField int64) (recBytes int64, padBytes int64) {
	// the record size is stored in the lower 56 bits of the 64-bit length
	recBytes = int64(uint64(lenField) & ^(uint64(0xff) << 56))
	// non-zero padding is indicated by set MSb / a negative length
	if lenField < 0 {
		// padding is stored in lower 3 bits of length MSB
		padBytes = int64((uint64(lenField) >> 56) & 0x7)
	}
	return recBytes, padBytes
}

// isTornEntry determines whether the last entry of the WAL was partially written
// and corrupted because of a torn write.
func (d *decoder) isTornEntry(data []byte) bool {
	if len(d.brs) != 1 {
		return false
	}

	fileOff := d.lastValidOff + frameSizeBytes
	curOff := 0
	chunks := [][]byte{}
	// split data on sector boundaries
	for curOff < len(data) {
		chunkLen := int(minSectorSize - (fileOff % minSectorSize))
		if chunkLen > len(data)-curOff {
			chunkLen = len(data) - curOff
		}
		chunks = append(chunks, data[curOff:curOff+chunkLen])
		fileOff += int64(chunkLen)
		curOff += chunkLen
	}

	// if any data for a sector chunk is all 0, it's a torn write
	for _, sect := range chunks {
		isZero := true
		for _, v := range sect {
			if v != 0 {
				isZero = false
				break
			}
		}
		if isZero {
			return true
		}
	}
	return false
}

func (d *decoder) updateCRC(prevCrc uint32) {
	d.crc = crc.New(prevCrc, crcTable)
}

func (d *decoder) lastCRC() uint32 {
	return d.crc.Sum32()
}

func (d *decoder) lastOffset() int64 { return d.lastValidOff }

func mustUnmarshalEntry(d []byte) raftpb.Entry {
	var e raftpb.Entry
	pbutil.MustUnmarshal(&e, d)
	return e
}

func mustUnmarshalState(d []byte) raftpb.HardState {
	var s raftpb.HardState
	pbutil.MustUnmarshal(&s, d)
	return s
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}
