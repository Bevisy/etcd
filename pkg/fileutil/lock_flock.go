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

// +build !windows,!plan9,!solaris

package fileutil

import (
	"os"
	"syscall"
)

func flockTryLockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if err == syscall.EWOULDBLOCK {
			err = ErrLocked
		}
		return nil, err
	}
	return &LockedFile{f}, nil
}

func flockLockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	// 打开或者创建并打开文件
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	// 添加文件锁：1.避免多个进程同时存在，导致访问冲突；2.程序意外中断，文件锁自动解锁
	//
	// LOCK_SH，共享锁，多个进程可以使用同一把锁，常被用作读共享锁；
	// LOCK_EX，排它锁，同时只允许一个进程使用，常被用作写锁；
	// LOCK_NB，遇到锁的表现，当采用排他锁的时候，默认 goroutine 会被阻塞等待锁被释放，采用 LOCK_NB 参数，可以让 goroutine 返回 Error;
	// LOCK_UN，释放锁；
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return &LockedFile{f}, err
}
