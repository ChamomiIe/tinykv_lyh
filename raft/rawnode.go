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
	"errors"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.

// Ready 封装了那些已准备好进行读取、保存到稳定存储、提交或发送给其他对等节点的日志条目和消息。
// Ready 中的所有字段均为只读字段。
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	// 节点当前的易变状态。
	// 如果没有状态更新，SoftState 将为 nil（空值）。
	// 无需使用或存储 SoftState。 包含lead和raftState两个字段
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	// 节点的当前状态，此状态需在发送消息 之前 保存到稳定存储中。
	// 如果没有状态更新，HardState 将等同于空状态。
	// 包含term、vote和committed三个字段
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	// 发msg前存到storage的entries
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	// CommittedEntries 是需要提交状态机的entries。
	// 这些条目之前已提交到稳定存储中。
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	// Messages 是在 Entries 提交到稳定存储后要发送的出站消息。
	// 如果它包含一个 MessageType_MsgSnapshot 消息，则应用程序必须在快照被接收或失败后报告给 raft，
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	PreSoft *SoftState
	PreHard pb.HardState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	// 1. 创建一个新的 Raft 实例
	raft := newRaft(config)
	// 2. 创建一个新的 RawNode 实例
	hardState := pb.HardState{
		Term:   raft.Term,
		Vote:   raft.Vote,
		Commit: raft.RaftLog.committed,
	}
	softState := &SoftState{
		Lead:      raft.Lead,
		RaftState: raft.State,
	}
	rn := &RawNode{
		Raft:    raft,
		PreHard: hardState,
		PreSoft: softState,
	}
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	log.Debugf("RawNode.Campaign, id: %d", rn.Raft.id)
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	log.DIYf("req", "RawNode.Propose, id: %d, data: %s", rn.Raft.id, string(data))
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	if !rn.HasReady() {
		return Ready{}
	}
	var softState *SoftState
	var hardState pb.HardState
	if rn.ChangeSoft() {
		softState = &SoftState{
			Lead:      rn.Raft.Lead,
			RaftState: rn.Raft.State,
		}
	} else {
		softState = nil
	}
	if rn.ChangeHard() {
		hardState = pb.HardState{
			Term:   rn.Raft.Term,
			Vote:   rn.Raft.Vote,
			Commit: rn.Raft.RaftLog.committed,
		}
	} else {
		hardState = pb.HardState{}
	}
	// firstindex := rn.Raft.RaftLog.firstIndex
	// stable := rn.Raft.RaftLog.stabled
	entries := rn.Raft.RaftLog.unstableEntries()
	committedEntries := rn.Raft.RaftLog.nextEnts()
	messages := rn.Raft.msgs
	if len(messages) == 0 {
		messages = nil
	}
	var snapshot pb.Snapshot
	if rn.Raft.RaftLog.pendingSnapshot == nil {
		snapshot = pb.Snapshot{}
	} else {
		snapshot = *rn.Raft.RaftLog.pendingSnapshot
	}
	ready := Ready{
		SoftState:        softState,
		HardState:        hardState,
		Entries:          entries,
		Snapshot:         snapshot,
		CommittedEntries: committedEntries,
		Messages:         messages,
	}
	return ready
}

func (rn *RawNode) ChangeSoft() bool {
	if rn.PreSoft.Lead != rn.Raft.Lead ||
		rn.PreSoft.RaftState != rn.Raft.State {
		return true
	}
	return false
}
func (rn *RawNode) ChangeHard() bool {
	if rn.PreHard.Term != rn.Raft.Term ||
		rn.PreHard.Commit != rn.Raft.RaftLog.committed ||
		rn.PreHard.Vote != rn.Raft.Vote {
		return true
	}
	return false
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	return len(rn.Raft.msgs) > 0 || //是否有需要发送的 Msg
		len(rn.Raft.RaftLog.unstableEntries()) > 0 || //是否有需要应用的条目
		len(rn.Raft.RaftLog.nextEnts()) > 0 || //是否有需要持久化的条目
		!IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) || //是否有需要应用的快照
		rn.ChangeSoft() || //是否有需要更新的软状态
		rn.ChangeHard() //是否有需要更新的硬状态
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	// 更新applied
	rn.Raft.RaftLog.applied = max(rn.PreHard.Commit, rn.Raft.RaftLog.applied)
	rn.Raft.RaftLog.applied = max(rd.Commit, rn.Raft.RaftLog.applied)
	rn.Raft.RaftLog.stabled = max(rn.PreHard.Commit, rn.Raft.RaftLog.stabled)
	rn.Raft.RaftLog.stabled = max(rd.Commit, rn.Raft.RaftLog.stabled)
	rn.PreHard = pb.HardState{
		Term:   rn.Raft.Term,
		Vote:   rn.Raft.Vote,
		Commit: rn.Raft.RaftLog.committed,
	}
	rn.PreSoft = &SoftState{
		Lead:      rn.Raft.Lead,
		RaftState: rn.Raft.State,
	}
	rn.Raft.msgs = make([]pb.Message, 0)
	// hardstate := pb.HardState{}
	// // entries := []pb.Entry{}
	// // committedEntries := rn.Raft.RaftLog.nextEnts()
	// rd.SoftState = nil
	// rd.HardState = hardstate
	// rd.Entries = nil
	// rd.CommittedEntries = nil
	// rd.Messages = rn.Raft.msgs
	// rn.Raft.msgs = nil
	// rd.Snapshot = pb.Snapshot{}
	return
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
