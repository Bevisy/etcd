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

package wal

import (
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/pkg/fileutil"

	"go.uber.org/zap"
)

// filePipeline pipelines allocating disk space
type filePipeline struct {
	lg *zap.Logger

	dir string // 存放临时文件的目录

	size int64 // 创建临时文件时预分配空间大小，默认 64MB

	count int // 当前 filepipeline 实例创建的文件数

	filec chan *fileutil.LockedFile // 新建的临时文件句柄会通过 filec 通道返回给 WAL 实例使用
	errc  chan error                // 当创建临时文件出现异常时，则将异常传递到 errc 通道中
	donec chan struct{}             // 当filePipeline.Close()被调用时，会关闭 donec 通道，从而通知 filePipeline 实例删除最后一次创建的临时文件
}

func newFilePipeline(lg *zap.Logger, dir string, fileSize int64) *filePipeline {
	fp := &filePipeline{
		lg:    lg,
		dir:   dir,
		size:  fileSize,
		filec: make(chan *fileutil.LockedFile),
		errc:  make(chan error, 1),
		donec: make(chan struct{}),
	}
	go fp.run()
	return fp
}

// Open returns a fresh file for writing. Rename the file before calling
// Open again or there will be file collisions.
//
// 返回最新的文件用于写入。在再次 Open 文件之前，重命名文件，避免文件冲突
func (fp *filePipeline) Open() (f *fileutil.LockedFile, err error) {
	select {
	case f = <-fp.filec: // 获取创建好的文件
	case err = <-fp.errc: // 如果创建临时文件异常，则获取异常并返回
	}
	return f, err
}

func (fp *filePipeline) Close() error {
	close(fp.donec)
	return <-fp.errc
}

func (fp *filePipeline) alloc() (f *fileutil.LockedFile, err error) {
	// count % 2 so this file isn't the same as the one last published
	// 为了防止与前一个创建的临时文件重名，新建临时文件的编号是 0 或是 1
	fpath := filepath.Join(fp.dir, fmt.Sprintf("%d.tmp", fp.count%2))
	// 创建临时文件
	if f, err = fileutil.LockFile(fpath, os.O_CREATE|os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return nil, err
	}
	// 预分配文件空间。如果节点不支持预分配，并不会报错
	if err = fileutil.Preallocate(f.File, fp.size, true); err != nil {
		if fp.lg != nil {
			fp.lg.Warn("failed to preallocate space when creating a new WAL", zap.Int64("size", fp.size), zap.Error(err))
		} else {
			plog.Errorf("failed to allocate space when creating new wal file (%v)", err)
		}
		f.Close() // 文件创建异常，关闭 donec 通道
		return nil, err
	}
	fp.count++    // 递增创建的文件数量
	return f, nil // 返回创建的临时文件
}

func (fp *filePipeline) run() {
	defer close(fp.errc)
	for {
		f, err := fp.alloc() // 创建临时文件
		if err != nil {
			fp.errc <- err // 创建异常，则写入 errc 通道
			return
		}
		select {
		case fp.filec <- f: // 创建的文件，写入 filec 通道
		case <-fp.donec: // 关闭时，删除最后一次创建的文件
			os.Remove(f.Name())
			f.Close()
			return
		}
	}
}
