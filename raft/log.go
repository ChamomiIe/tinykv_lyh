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

package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// “committed”（已提交）指的是已知在多数节点的稳定存储中存在的最高日志位置。
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// “applied”（已应用）指的是应用程序已被指示应用到其状态机的最高日志位置。
	// 不变性条件：已应用的日志位置必须小于或等于已提交的日志位置。
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// 索引小于或等于 stabled 的日志条目已被持久化到存储中。
	// 它用于记录尚未被存储持久化的日志。
	// 每次处理 Ready 时，未持久化的日志都会被包含在内。
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	last, error := storage.LastIndex()
	if error != nil {
		panic("storage last index error")
	}
	first, error := storage.FirstIndex()
	if error != nil {
		panic("storage first index error")
	}
	var applied uint64
	// 只有dummy entries时
	// applied如何处理？
	if first == last+1 {
		first = last
		applied = last
	} else {
		applied = first - 1
	}
	entries, error := storage.Entries(first, last+1)
	hardstate, _, error := storage.InitialState()
	raftlog := RaftLog{
		storage:         storage,
		committed:       hardstate.Commit,
		applied:         applied,
		stabled:         last,
		entries:         entries,
		pendingSnapshot: nil,
		firstIndex:      first,
	}
	log.Debug("new log firstIndex, last, applied, committed, stabled", first, last, applied, hardstate.Commit, last)
	return &raftlog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {

	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
// allEntries 函数返回所有未被压缩的日志条目。
// 注意，返回值中要排除任何虚拟的日志条目。
// 注意，这是你需要实现的测试桩函数之一。
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

func (l *RaftLog) EntriesFromIndex(first uint64, last uint64) []pb.Entry {
	// Your Code Here (2A).
	return l.entries[first-l.firstIndex : last+1-l.firstIndex]
}

func (l *RaftLog) EntriesFromFirst(first uint64) []pb.Entry {
	// Your Code Here (2A).
	return l.entries[first-l.firstIndex:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	l.Changefirst()
	if l.stabled <= l.firstIndex {
		return make([]pb.Entry, 0)
	}
	entries := l.entries[l.stabled+1-l.firstIndex:]
	return entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	l.Changefirst()
	if l.committed <= l.applied {
		return nil
	}
	entries := l.entries[l.applied+1-l.firstIndex : l.committed+1-l.firstIndex]
	return entries
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	l.Changefirst()
	if len(l.entries) == 0 {
		return l.firstIndex
	}
	return l.firstIndex + uint64(len(l.entries)) - 1
}

func (l *RaftLog) Changefirst() {
	if len(l.entries) != 0 {
		l.firstIndex = l.entries[0].Index
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	l.Changefirst()
	if i > l.LastIndex() {
		return 0, ErrCompacted
	} else if i < l.firstIndex {
		// 快照中找？
		return 0, ErrCompacted
	}
	if len(l.entries) == 0 {
		return 0, ErrCompacted
	}
	term := l.entries[i-l.firstIndex].Term
	return term, nil
}
