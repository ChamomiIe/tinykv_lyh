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

	"math/rand"
	"time"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	//peers 包含了 Raft 集群中所有节点（包括自身）的 ID。仅在启动一个新的 Raft 集群时才应设置该值。
	// 如果从之前的配置重启 Raft 时设置了 peers，程序将会触发恐慌（panic）。目前，peers 是私有变量，仅用于测试。
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	//选举超时时间，十倍心跳周期
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	//心跳周期
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	// Storage（存储）是 Raft 算法的数据存储组件。Raft 会生成日志条目和状态信息，并将它们存储到 Storage 中。
	// 当有需要时，Raft 会从 Storage 中读取持久化的日志条目和状态信息。在重启时，Raft 会从 Storage 中读取之前保存的状态和配置信息。
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	// Applied 是最后一次应用的索引。只有在重启 Raft 时才应设置它。Raft 不会向应用程序返回小于或等于 Applied 的日志条目。
	// 如果在重启时未设置 Applied，Raft 可能会返回之前已应用的日志条目。这是一个与应用程序紧密相关的配置。
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
// “Progress”（进度）代表了在领导者视角下某个追随者的进展情况。领导者会维护所有追随者的进度，并根据每个追随者的进度向其发送日志条目。
type Progress struct {
	//match表示该节点已经成功复制的日志的最大索引。
	//next表示下一次需要发送给该节点的日志索引。
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes     map[uint64]bool
	rejectnum int
	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	// 自上次达到心跳超时时间以来所经过的计时周期数。
	// 只有领导者节点会维护心跳已过周期数（heartbeatElapsed）。
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	// 当节点处于领导者或候选者状态时，该值表示自上次选举超时后所经过的时间周期数。
	// 当节点处于追随者状态时，该值表示自上次选举超时或收到当前领导者的有效消息后所经过的时间周期数。
	// 选出新领导人后清零
	electionElapsed int
	randomTimeout   int
	// randomTimeout   int
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	// 当 leadTransferee 的值不为 0 时，它代表领导者转移目标的 ID。
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	// 同一时间只能有一项配置变更处于待处理状态（已记录在日志中，但尚未应用）。
	// 这一限制通过 PendingConfIndex 来实现，该值会被设置为大于或等于最新待处理配置变更的日志索引（如果存在待处理变更的话）。
	// 仅当领导者的已应用索引大于该值时，才允许提出新的配置变更提议。
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardstate, _, _ := c.Storage.InitialState()
	prs := make(map[uint64]*Progress)
	if len(c.peers) == 0 {
		_, confstate, _ := c.Storage.InitialState()
		for _, id := range confstate.Nodes {
			c.peers = append(c.peers, id)
		}
	}
	for i := 0; i < len(c.peers); i++ {
		prs[c.peers[i]] = &Progress{
			Match: 0,
			Next:  0,
		}
	}
	votes := make(map[uint64]bool)
	for _, id := range c.peers {
		votes[id] = false
	}
	// 创建一个局部随机数生成器
	randomGen := rand.New(rand.NewSource(time.Now().UnixNano()))
	// 使用局部生成器生成随机数 生成在 [et, 2*et) 范围内的随机超时时间
	randomTimeout := randomGen.Intn(c.ElectionTick) + c.ElectionTick
	raft := &Raft{
		id:               c.ID,
		Term:             hardstate.Term,
		Vote:             hardstate.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              prs,
		State:            StateFollower,
		votes:            votes,
		rejectnum:        0,
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   None,
		PendingConfIndex: 0,
		randomTimeout:    randomTimeout,
	}
	log.Debug("raft newRaft id: term: vote: state: log.first: log.commit:",
		raft.id, raft.Term, raft.Vote, raft.State, raft.RaftLog.firstIndex, raft.RaftLog.committed)
	log.Debug("log.entries:", raft.RaftLog.entries)
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// sendAppend 函数会向指定的对等节点发送一个附带新日志条目（如果有的话）以及
// 当前提交索引的追加日志远程过程调用（RPC）。若消息已成功发送，则返回 true。
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// 检查目标节点是否在集群中
	pr, ok := r.Prs[to]
	if !ok {
		return false
	}
	// 获取需要发送的日志条目
	log.DIYf("msg", "sendAppend, %d send append to %d, pr: %v", r.id, to, pr)
	nextIndex := pr.Next
	// match := pr.Match
	// 取上一条日志的term和index
	var logterm, index uint64
	if nextIndex == 0 {
		logterm = 0
		index = 0
	} else {
		// last为第一个时是否需要从快照中取？
		logterm, _ = r.RaftLog.Term(nextIndex - 1)
		index = nextIndex - 1
	}
	// log.DIYf("msg", "sendAppend, %d send append to %d, nextIndex: %d, logterm: %d, index: %d", r.id, to, nextIndex, logterm, index)
	// log.DIYf("msg", "r.lastindex %d r.term %d", r.RaftLog.LastIndex(), r.Term)
	// leader与follower已经同步时，仅用于传递commit
	if nextIndex > r.RaftLog.LastIndex() {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			To:      to,
			From:    r.id,
			Term:    r.Term,
			Commit:  r.RaftLog.committed,
			Entries: nil,
			Index:   index,
			LogTerm: logterm,
		}
		r.msgs = append(r.msgs, msg)
		return true
	}
	if nextIndex < r.RaftLog.firstIndex {
		//发送快照
		snapshot, err := r.RaftLog.storage.Snapshot()
		if err != nil {
			return false
		}
		msg := pb.Message{
			MsgType:  pb.MessageType_MsgSnapshot,
			To:       to,
			From:     r.id,
			Term:     r.Term,
			Snapshot: &snapshot,
		}
		r.msgs = append(r.msgs, msg)
		return true
	}
	//send从nextIndex开始到lastIndex的entries
	// logterm为match的term
	entriesToSend := r.RaftLog.EntriesFromFirst(nextIndex)
	// lastIndex := r.RaftLog.LastIndex()
	// Convert entriesToSend from []eraftpb.Entry to []*eraftpb.Entry
	var entriesToSendPtr []*pb.Entry
	for i := range entriesToSend {
		entriesToSendPtr = append(entriesToSendPtr, &entriesToSend[i])
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logterm,
		Index:   index,
		Entries: entriesToSendPtr,
		Commit:  r.RaftLog.committed,
	}
	log.DIYf("msg", "sendAppend, %d send append to %d, msg: %v", r.id, to, msg)
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// 检查目标节点是否在集群中
	_, ok := r.Prs[to]
	if !ok {
		return
	}
	var term uint64
	if r.RaftLog.LastIndex() == 0 {
		term = 0
	} else {
		lastindex := r.RaftLog.LastIndex()
		term, _ = r.RaftLog.Term(lastindex)
	}
	// 获取需要发送的日志条目
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: term,
		Entries: make([]*pb.Entry, 0),
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.leaderTick()
	case StateCandidate:
		r.candidateTick()
	case StateFollower:
		r.followerTick()
	}
}

func (r *Raft) leaderTick() {
	// Your Code Here (2A).
	log.DIYf("tick", "id %d leaderTick electionElapsed: %d, heartbeatElapsed: %d, randomTimeout: %d", r.id, r.electionElapsed, r.heartbeatElapsed, r.randomTimeout)
	r.electionElapsed++
	r.heartbeatElapsed++
	// 检查是否需要发送心跳消息
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		// for id := range r.Prs {
		// 	if id != r.id {
		// 		r.sendHeartbeat(id)
		// 	}
		// }
		// msg := pb.Message{
		// 	MsgType: pb.MessageType_MsgBeat,
		// 	From:    r.id,
		// 	Term:    r.Term,
		// 	Commit:  r.RaftLog.committed,
		// }
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			msg := pb.Message{
				MsgType: pb.MessageType_MsgHeartbeat,
				To:      id,
				From:    r.id,
				Term:    r.Term,
				Commit:  r.RaftLog.committed,
			}
			r.msgs = append(r.msgs, msg)
		}

	}
	//拒绝所有投票
}

func (r *Raft) candidateTick() {
	// Your Code Here (2A).
	log.DIYf("tick", "id %d candidateTick electionElapsed: %d, randomTimeout: %d", r.id, r.electionElapsed, r.randomTimeout)
	r.electionElapsed++
	//拒绝所有投票

	//收到leader心跳则转为follower，electionElapsed清零

	//收到投票则转为leader

	// 检查是否需要进行选举
	if r.electionElapsed >= r.randomTimeout {
		r.becomeCandidate()
	}
}

func (r *Raft) followerTick() {
	log.DIYf("tick", "id %d followerTick electionElapsed: %d, randomTimeout: %d", r.id, r.electionElapsed, r.randomTimeout)
	// Your Code Here (2A).
	r.electionElapsed++
	//收到心跳或追加条目，electionElapsed清零

	//处理投票

	// 检查是否需要进行选举
	if r.electionElapsed >= r.randomTimeout {
		// r.electionElapsed = 0
		r.becomeCandidate()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.State = StateFollower
	r.rejectnum = 0
	r.Lead = lead
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	if r.State != StateFollower {
		// 如果当前节点不是follower，则转为follower
		r.Vote = None
		// 创建一个局部随机数生成器
		randomGen := rand.New(rand.NewSource(time.Now().UnixNano()))
		// // 使用局部生成器生成随机数 生成在 [et, 2*et) 范围内的随机超时时间
		r.randomTimeout = randomGen.Intn(r.electionTimeout) + r.electionTimeout
		for id := range r.Prs {
			r.votes[id] = false
		}
	}
	// r.Term = m.Term
	// r.Vote = None
	// // 创建一个局部随机数生成器
	// randomGen := rand.New(rand.NewSource(time.Now().UnixNano()))
	// // // 使用局部生成器生成随机数 生成在 [et, 2*et) 范围内的随机超时时间
	// r.randomTimeout = randomGen.Intn(r.electionTimeout) + r.electionTimeout
	// for id := range r.Prs {
	// 	r.votes[id] = false
	// }
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Vote = r.id
	r.State = StateCandidate
	r.votes[r.id] = true
	// 计算范围的大小
	// 计算随机数范围：0 到 et（含）
	// max := big.NewInt(int64(r.electionTimeout))
	// // 生成 [0, et] 的随机数
	// n, err := rand.Int(rand.Reader, max)
	// if err != nil {
	// 	return
	// }
	// // 将结果映射到 [et, 2*et]
	// electionTimeout := int(n.Int64()) + r.electionTimeout
	if r.electionElapsed >= r.randomTimeout {
		r.electionElapsed = 0
		r.heartbeatElapsed = 0
		// Send RequestVote RPC to all peers
		var logterm, index uint64
		lastIndex := r.RaftLog.LastIndex()
		if lastIndex == 0 {
			logterm = 0
			index = 0
		} else {
			// last为第一个时是否需要从快照中取？
			logterm, _ = r.RaftLog.Term(lastIndex)
			index = lastIndex
		}
		for id := range r.Prs {
			if id != r.id {
				msg := pb.Message{
					MsgType: pb.MessageType_MsgRequestVote,
					To:      id,
					From:    r.id,
					Term:    r.Term,
					LogTerm: logterm,
					Index:   index,
				}
				r.msgs = append(r.msgs, msg)
			}
		}
		log.DIYf("msg", "becomeCandidate, %d send msgs to peers, msgs: %v", r.id, r.msgs)
	}
	// 创建一个局部随机数生成器
	randomGen := rand.New(rand.NewSource(time.Now().UnixNano()))
	// 使用局部生成器生成随机数 生成在 [et, 2*et) 范围内的随机超时时间
	r.randomTimeout = randomGen.Intn(r.electionTimeout) + r.electionTimeout
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.Lead = r.id
	r.State = StateLeader
	// r.Vote = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// Send initial empty entry to trigger replication
	// 发送初始空日志条目以触发复制
	// msg := pb.Message{
	// 	MsgType: pb.MessageType_MsgBeat,
	// 	To:      r.id,
	// 	From:    r.id,
	// 	Term:    r.Term,
	// 	Commit:  r.RaftLog.committed,
	// 	Entries: make([]*pb.Entry, 0),
	// }
	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		r.Prs[id].Match = 0
	}
	entries := make([]*pb.Entry, 1)
	entries[0] = &pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
		// Data 留空表示这是一个 noop entry
	}
	log.DIYf("msg", "becomeLeader, %d become leader, send noop entry to peers, last:%v entries: %v", r.id, r.RaftLog.LastIndex(), entries)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
		Entries: entries,
	}
	r.Step(msg)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// 处理消息
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		if m.MsgType == 0 {
			//MessageType_MsgHup：选举超时，开启新选举
			r.electionElapsed = 20
			r.becomeCandidate()
			if len(r.votes) == 1 {
				r.becomeLeader()
			}
			return nil
		}
		if m.MsgType == 3 {
			r.handleAppendEntries(m)
			log.DIYf("msg", "handleAppendEntries, %d handleappendentries return msg: %v", r.id, r.msgs)
			log.DIYf("msg", "r.id:%d, r.committed: %d, r.lastentry: %v", r.id, r.RaftLog.committed, r.RaftLog.entries[r.RaftLog.LastIndex()-r.RaftLog.firstIndex])
			return nil
		}
		if m.MsgType == 5 {
			err := r.handleRequestVote(m)
			log.DIYf("msg", "handleRequestVote, %d handlerequestvote return msg: %v", r.id, r.msgs)
			return err
		}
		if m.MsgType == 7 {
			//MessageType_MsgSnapshot,快照消息
			err := r.handleMsgSnapshot(m)
			return err
		}
		if m.MsgType == 8 {
			//MessageType_MsgHeartbeat,发送心跳
			r.handleHeartbeat(m)
			return nil
		}
	case StateCandidate:
		if m.MsgType == 0 {
			//MessageType_MsgHup：选举超时，开启新选举
			r.electionElapsed = 20
			r.becomeCandidate()
			if len(r.votes) == 1 {
				r.becomeLeader()
			}
			return nil
		}
		if m.MsgType == 3 {
			r.handleAppendEntries(m)
			log.DIYf("msg", "handleAppendEntries, %d handleappendentries return msg: %v", r.id, r.msgs)
			return nil
		}
		if m.MsgType == 5 {
			err := r.handleRequestVote(m)
			return err
		}
		if m.MsgType == 6 {
			// MessageType_MsgRequestVoteResponse,投票响应
			err := r.handleVoteResponse(m)
			log.DIYf("msg", "handleRequestVote, %d handlerequestvote state %v from %v return msg: %v", r.id, r.State, m.From, r.msgs)
			return err
		}
		if m.MsgType == 7 {
			//MessageType_MsgSnapshot,快照消息
			err := r.handleMsgSnapshot(m)
			return err
		}
		if m.MsgType == 8 {
			//MessageType_MsgHeartbeat,发送心跳
			r.handleHeartbeat(m)
			return nil
		}
	case StateLeader:
		// MessageType_MsgBeat,要求leader发送心跳
		if m.MsgType == 1 {
			// 发送心跳消息
			for id := range r.Prs {
				if id != r.id {
					r.sendHeartbeat(id)
				}
			}
			return nil
		}
		if m.MsgType == 2 {
			// MessageType_MsgPropose,本地消息,提议在leader的log entries中追加数据
			for i := 0; i < len(m.Entries); i++ {
				m.Entries[i].Term = r.Term
				m.Entries[i].Index = r.RaftLog.LastIndex() + 1
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
				log.DIYf("req", "Propose, %d propose msg: %v", r.id, *m.Entries[i])
			}
			r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
			r.Prs[r.id].Match = r.RaftLog.LastIndex()
			for id := range r.Prs {
				if id != r.id {
					r.sendAppend(id)
				}
			}
			// 集群大小为1时直接更新commit
			if len(r.Prs) == 1 {
				r.RaftLog.committed = r.RaftLog.LastIndex()
			}
			return nil
		}
		// 发送消息？
		if m.MsgType == 3 {
			// MessageType_MsgAppend,追加日志
			// r.msgs = append(r.msgs, m)
			// r.sendAppend(m.To)
			r.handleAppendEntries(m)
			return nil
		}
		if m.MsgType == 4 {
			//MessageType_MsgAppendResponse,追加日志响应
			if m.Reject {
				// 如果拒绝，则往前找一个
				prs := r.Prs[m.From]
				prs.Next--
				if prs.Next < 1 || prs.Next < prs.Match {
					return errors.New("next index err")
				}
				r.sendAppend(m.From)
			} else {
				// 如果接受，则更新目标节点的匹配索引
				pr := r.Prs[m.From]
				pr.Match = m.Index
				pr.Next = m.Index + 1
				// 仅提交当前term的日志
				term, _ := r.RaftLog.Term(m.Index)
				if term != r.Term {
					return nil
				}
				var commit int = 0
				// 更新r.RaftLog.committed字段
				for id := range r.Prs {
					if r.Prs[id].Match >= m.Index && r.RaftLog.committed < m.Index {
						commit++
					}
				}
				if commit > len(r.Prs)/2 {
					// 提交日志
					r.RaftLog.committed = m.Index
					// 发心跳或者appendentries来同步commit
					for id := range r.Prs {
						if id != r.id {
							r.sendAppend(id)
						}
					}
					// for id := range r.Prs {
					// 	if id != r.id {
					// 		r.sendHeartbeat(id)
					// 	}
					// }
				}
			}
			return nil
		}
		if m.MsgType == 5 {
			if m.Term > r.Term {
				r.State = StateFollower
				r.Vote = None
				err := r.handleRequestVote(m)
				return err
			}
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			}
			r.msgs = append(r.msgs, msg)
			return nil
		}
		if m.MsgType == 6 {
			// MessageType_MsgRequestVoteResponse,投票响应
			// err := r.handleVoteResponse(m)
			return nil
		}
		if m.MsgType == 9 {
			//MessageType_MsgHeartbeatResponse,心跳响应
			err := r.handleHeartResponse(m)
			return err
		}
	}
	return nil
}

func (r *Raft) handleRequestVote(m pb.Message) error {
	//MessageType_MsgRequestVote 请求投票
	// 如果当前term已经投票过，则拒绝
	if m.Term == r.Term && r.Vote != None && r.Vote != m.From {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return nil
	}
	// 检查当前节点的任期是否小于接收到的消息的任期
	if m.Term < r.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return nil
	} else {
		// 如果接收到的消息的任期大于等于当前节点的任期，检查上一条日志的大小
		// r.Term = m.Term
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		}
		lastindex := r.RaftLog.LastIndex()
		var lastterm uint64
		lastterm, _ = r.RaftLog.Term(lastindex)
		//对比上一条logterm大小
		if m.LogTerm > lastterm {
			// 投票并更新当前节点的vote
			r.Vote = m.From
			// r.State = StateFollower
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  false,
			}
			r.msgs = append(r.msgs, msg)
			return nil
		}
		if m.LogTerm == lastterm {
			//term相等则比较index
			if m.Index >= lastindex {
				r.Vote = m.From
				r.State = StateFollower
				msg := pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  false,
				}
				r.msgs = append(r.msgs, msg)
				return nil
			} else {
				// 如果接收到的消息的index小于当前节点的最后index，则拒绝投票
				msg := pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  true,
				}
				r.msgs = append(r.msgs, msg)
				return nil
			}
		}
		if m.LogTerm < lastterm {
			// 如果接收到的消息的term小于当前节点的最后term，则拒绝投票
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			}
			r.msgs = append(r.msgs, msg)
			return nil
		}
	}
	return nil
}

func (r *Raft) handleVoteResponse(m pb.Message) error {
	//MessageType_MsgRequestVoteResponse
	if m.Reject {
		r.rejectnum++
		// 如果拒绝投票的数量超过半数，则转为follower
		if r.rejectnum > len(r.Prs)/2 {
			r.State = StateFollower
			r.rejectnum = 0
			r.Vote = None
			r.Lead = None
			r.electionElapsed = 0
			r.heartbeatElapsed = 0
			if m.Term > r.Term {
				r.Term = m.Term
			}
			// 创建一个局部随机数生成器
			randomGen := rand.New(rand.NewSource(time.Now().UnixNano()))
			// 使用局部生成器生成随机数 生成在 [et, 2*et) 范围内的随机超时时间
			r.randomTimeout = randomGen.Intn(r.electionTimeout) + r.electionTimeout
			for id := range r.Prs {
				r.votes[id] = false
			}
			return nil
		}
		r.votes[m.From] = false
		return nil
	}
	// 对应votes记为true，超过半数投票通过则成为leader，heartelapsed和electionelapsed清零
	r.votes[m.From] = true
	votes := 0
	for _, v := range r.votes {
		if v {
			votes++
		}
	}
	if votes > len(r.Prs)/2 {
		r.rejectnum = 0
		r.becomeLeader()
	}
	return nil
}

func (r *Raft) handleMsgSnapshot(m pb.Message) error {
	//MessageType_MsgSnapshot
	return nil
}

func (r *Raft) handleHeartResponse(m pb.Message) error {
	//MessageType_MsgHeartbeatResponse
	// if m.Reject {
	// 	// 如果拒绝，则往前找一个
	// 	r.Prs[m.To].Next--
	// 	prs := r.Prs[m.To]
	// 	if prs.Next < 1 || prs.Next < prs.Match {
	// 		return errors.New("next index error")
	// 	}
	// 	r.sendAppend(m.To)
	// } else {
	// 	// 如果接受，则更新目标节点的匹配索引
	// 	pr := r.Prs[m.To]
	// 	pr.Match = m.Index
	// 	pr.Next = m.Index + 1
	// }
	if m.Term > r.Term {
		// 如果接收到的消息的任期大于当前节点的任期，则更新当前节点的任期和领导者ID
		// 如果当前节点不是follower，则转为follower
		r.becomeFollower(m.Term, m.From)
	}
	if m.Reject {
		// msg := pb.Message{
		// 	MsgType: pb.MessageType_MsgHeartbeat,
		// 	To:      m.From,
		// 	Term:    r.Term,
		// 	From:    r.id,
		// }
		// r.msgs = append(r.msgs, msg)
		r.sendAppend(m.From)
	}
	return nil
}

func (r *Raft) updateCommit(m pb.Message) {
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	if m.Entries == nil {
		r.RaftLog.committed = m.Index
	}
}

func (r *Raft) updateIndex() uint64 {
	var index uint64
	if len(r.RaftLog.entries) > 0 {
		index = r.RaftLog.entries[len(r.RaftLog.entries)-1].Index
	} else {
		index = 0
	}
	return index
}

func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	// 检查当前节点的任期是否小于接收到的消息的任期
	if m.Term < r.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return
	}
	// 如果收到快照则应用快照
	if m.Snapshot != nil {
		// 应用快照
		r.handleSnapshot(m)
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Index:   r.RaftLog.firstIndex,
			Reject:  false,
		}
		r.msgs = append(r.msgs, msg)
		return
	}
	// 如果接收到的消息的任期大于等于当前节点的任期，则更新当前节点的任期和领导者ID
	// 如果当前节点不是follower，则转为follower
	r.becomeFollower(m.Term, m.From)
	// 匹配raftlog entries中的日志条目与接收到的消息中的日志条目 term和index
	// entries := r.RaftLog.entries
	currterm, _ := r.RaftLog.Term(m.Index)
	// 如果当前节点term不能匹配上，则拒绝
	if currterm != m.LogTerm {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return
	}
	// 如果匹配成功，更新当前节点的任期和领导者ID
	r.Term = m.Term
	r.Vote = m.From
	r.Lead = m.From
	// 如果entries长度为0，直接返回，否则进行append
	// 如果已经存在的日志条目与接收到的日志条目冲突，则删除冲突的日志条目
	// 全部相同时不覆盖,flag为真是覆盖，遍历m中所有entry，同时按顺序遍历raftlog中的entry，不相同时flag为真
	var flag bool = false
	var pos uint64

	for j := 0; j < len(m.Entries); j++ {
		if m.Entries[j].Index <= r.RaftLog.LastIndex() {
			// 如果接收到的日志条目与当前节点的日志条目冲突，则删除冲突的日志条目。否则直接返回。
			term, _ := r.RaftLog.Term(m.Entries[j].Index)
			if m.Entries[j].Term != term {
				flag = true
				pos = m.Entries[j].Index
				r.RaftLog.entries = r.RaftLog.entries[:pos-r.RaftLog.firstIndex]
				break
			}
		} else {
			flag = true
			break
		}
	}
	if m.Index+1 > r.RaftLog.LastIndex() {
		flag = true
	}
	if flag {
		//  index考虑大于当前节点的情况，返回err？
		// 如果删掉了stable的部分，更新stable
		// if r.RaftLog.stabled > m.Index {
		// 	r.RaftLog.stabled = m.Index
		// }
		if r.RaftLog.stabled > pos-1 {
			r.RaftLog.stabled = pos - 1
		}
		for _, entry := range m.Entries {
			// 添加新的日志条目
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
	}
	// 更新match，将match传回leader
	index := r.updateIndex()
	// 更新当前节点的提交索引
	r.updateCommit(m)
	// 不用更新直接返回
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   index,
		Reject:  false,
	}
	r.msgs = append(r.msgs, msg)
	return
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	if m.Term > r.Term {
		// 如果接收到的消息的任期大于当前节点的任期，则更新当前节点的任期和领导者ID
		// 如果当前节点不是follower，则转为follower
		r.becomeFollower(m.Term, m.From)

	}
	// 消息的term小于当前节点，认为消息过期，拒绝
	if m.Term < r.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return
	}
	r.State = StateFollower
	// m中为前一个匹配的索引和任期,未匹配到则拒绝
	entries := r.RaftLog.entries
	for i := 0; i < len(entries); i++ {
		//未匹配到则拒绝
		if m.Index > r.RaftLog.LastIndex() {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgHeartbeatResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			}
			r.msgs = append(r.msgs, msg)
			return
		}
		if (r.RaftLog.entries[i].Term == m.LogTerm && r.RaftLog.entries[i].Index == m.Index) || (m.Index == 0 && m.LogTerm == 0) {
			//匹配成功
			// 更新当前节点的提交索引
			r.updateCommit(m)
			// 如果commit大于当前节点的已应用索引，则更新已applied并应用状态机
			// if r.RaftLog.committed > r.RaftLog.applied {
			// 	r.RaftLog.applied++
			// 	// 应用状态机（保存快照？）
			// }
			msg := pb.Message{
				MsgType: pb.MessageType_MsgHeartbeatResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  false,
			}
			r.msgs = append(r.msgs, msg)
			return
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if m.Snapshot == nil {
		return
	}
	// 如果接收到的快照的任期大于当前节点的任期，则更新当前节点的任期和领导者ID
	// if m.Snapshot.Metadata.Term > r.Term {
	// 	r.Term = m.Snapshot.Metadata.Term
	// 	r.Vote = None
	// 	r.State = StateFollower
	// }
	r.Term = m.Snapshot.Metadata.Term
	r.Vote = m.From
	r.State = StateFollower
	r.Lead = m.From
	// log怎么处理？
	r.RaftLog.entries = make([]pb.Entry, 0)
	r.RaftLog.firstIndex = m.Snapshot.Metadata.Index
	r.RaftLog.stabled = m.Snapshot.Metadata.Index
	r.RaftLog.committed = m.Snapshot.Metadata.Index
	// r.RaftLog.entries = append(r.RaftLog.entries, *m.Snapshot.Data...)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
