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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/samber/lo"
	"math/rand"
	"time"
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
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
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
type Progress struct {
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
	votes map[uint64]bool

	winVoteNum int

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
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	prs := make(map[uint64]*Progress)
	for _, p := range c.peers {
		prs[p] = &Progress{}
	}
	r := &Raft{
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		RaftLog:          newLog(c.Storage),
		Prs:              prs,
		votes:            map[uint64]bool{},
	}
	hardState, _, err := r.RaftLog.storage.InitialState()
	if err != nil {
		return nil
	}
	r.Vote = hardState.Vote
	r.RaftLog.committed = hardState.GetCommit()
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	if to == r.id {
		r.Prs[to].Match = r.Prs[to].Next
		r.Prs[to].Next += 1
		return true
	}
	// Your Code Here (2A).
	switch r.State {
	case StateCandidate:
		var lastEntity pb.Entry
		if l := len(r.RaftLog.entries); l > 0 {
			lastEntity = r.RaftLog.entries[l-1]
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: to, Term: r.Term, Index: lastEntity.Index, LogTerm: lastEntity.Term,
		})
	case StateLeader:
		peer := r.Prs[to]
		sendEntries := make([]*pb.Entry, 0)
		var sendIdx int
		peer.Match = peer.Next
		for i, entry := range r.RaftLog.entries {
			if entry.Index == peer.Next {
				sendIdx = i
				break
			}
		}
		if sendIdx < len(r.RaftLog.entries) {
			for i := sendIdx; i < len(r.RaftLog.entries); i++ {
				r.RaftLog.proposeResp[r.RaftLog.entries[i].Index] = 1
				sendEntries = append(sendEntries, &pb.Entry{
					EntryType: r.RaftLog.entries[i].EntryType,
					Term:      r.RaftLog.entries[i].Term,
					Index:     r.RaftLog.entries[i].Index,
					Data:      r.RaftLog.entries[i].Data,
				})
			}
		} else {
			sendEntries = append(sendEntries, &pb.Entry{
				EntryType: pb.EntryType_EntryNormal,
				Term:      r.Term,
				Index:     r.RaftLog.LastIndex(),
			})
			r.RaftLog.proposeResp[sendEntries[0].Index] = 1
		}
		var lastIndex, logTerm uint64
		if sendIdx-1 < len(r.RaftLog.entries) && sendIdx > 0 {
			lastIndex = r.RaftLog.entries[sendIdx-1].Index
			logTerm = r.RaftLog.entries[sendIdx-1].Term
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppend, From: r.id, To: to, Term: r.Term, Entries: sendEntries,
			LogTerm: logTerm, Index: lastIndex, Commit: r.RaftLog.committed,
		})
		peer.Next = r.RaftLog.LastIndex() + 1
	}
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		Term: r.Term, From: r.id, To: to, MsgType: pb.MessageType_MsgHeartbeat, Commit: r.RaftLog.committed,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed += 1
	r.heartbeatElapsed += 1
	switch r.State {
	case StateLeader:
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			for p := range r.Prs {
				if p == r.id {
					continue
				}
				r.sendHeartbeat(p)
			}
		}
	default:
		et := 10
		if r.electionElapsed > r.electionTimeout {
			r.becomeCandidate()
			for p := range r.Prs {
				r.sendAppend(p)
			}
			r.electionElapsed = 0
			rand.Seed(time.Now().UnixNano())
			r.electionTimeout = et + rand.Intn(et)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.winVoteNum = 0
	r.votes = map[uint64]bool{}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.votes[r.id] = true
	r.Vote = r.id
	r.winVoteNum = 1
	r.Term += 1
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.winVoteNum = 0
	r.State = StateLeader
	idx := r.RaftLog.LastIndex() + 1
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		EntryType: pb.EntryType_EntryNormal, Term: r.Term, Index: idx})
	for p, process := range r.Prs {
		process.Next = idx
		r.sendAppend(p)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.processMsgHup()
		case pb.MessageType_MsgRequestVote:
			r.handleVoteMsg(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.RaftLog.committed = m.Commit
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.handleVoteMsg(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.votes[m.From] = !m.Reject
			if !m.Reject {
				r.winVoteNum += 1
			}
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHup:
			r.processMsgHup()
		}
		if r.winVoteNum*2 > len(r.Prs) {
			r.becomeLeader()
		} else if (len(r.votes)-r.winVoteNum)*2 > len(r.Prs) {
			r.becomeFollower(m.Term, m.From)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.handleVoteMsg(m)
		case pb.MessageType_MsgBeat:
			for p := range r.Prs {
				if r.id == p {
					continue
				}
				r.sendHeartbeat(p)
			}
		case pb.MessageType_MsgPropose:
			r.handleProposeMsg(m)
		case pb.MessageType_MsgAppendResponse:
			if m.Reject {
				r.Prs[m.From].Next = m.Index
				r.sendAppend(m.From)
			} else {
				r.RaftLog.proposeResp[m.Index] += 1
				r.Prs[m.From].Match = m.Index
				if r.RaftLog.proposeResp[m.Index]*2 > uint64(len(r.Prs)) {
					if r.RaftLog.committed < m.Index {
						r.RaftLog.committed = lo.Max([]uint64{r.RaftLog.committed, m.Index})
						delete(r.RaftLog.proposeResp, m.Index)
						r.sendCommit()
					}
				}
			}
		}
	}
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		//return nil
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	reject := true
	r.Lead = m.From
	var match pb.Entry
	matchIdx := -1
	for i := len(r.RaftLog.entries) - 1; i >= 0; i-- {
		entity := r.RaftLog.entries[i]
		if entity.Index == m.Index && entity.Term == m.LogTerm {
			match = entity
			matchIdx = i
			break
		}
	}
	if (match.Term == m.LogTerm && matchIdx >= 0) || (m.LogTerm == 0 && m.Index == 0) {
		reject = false
		//r.RaftLog.committed = m.Commit
		//r.RaftLog.entries = r.RaftLog.entries[0:matchIdx]
		//r.RaftLog.stabled = r.RaftLog.committed
		if match.Term == m.LogTerm {
			matchIdx++
		}
		for _, e := range m.Entries {
			newEntity := pb.Entry{
				EntryType: e.EntryType,
				Term:      e.Term,
				Index:     e.Index,
				Data:      e.Data,
			}
			if len(r.RaftLog.entries) > matchIdx {
				oldEntity := r.RaftLog.entries[matchIdx]
				r.RaftLog.entries[matchIdx] = newEntity
				if oldEntity.Term < newEntity.Term {
					if r.RaftLog.stabled >= oldEntity.Index {
						r.RaftLog.stabled = oldEntity.Index - 1
					}
					r.RaftLog.entries = r.RaftLog.entries[0 : matchIdx+1]
				}

			} else {
				r.RaftLog.entries = append(r.RaftLog.entries, newEntity)
			}
			matchIdx++
		}
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = lo.Min([]uint64{m.Commit, m.Index + uint64(len(m.Entries))})
		} else {
			r.RaftLog.committed = m.Commit
		}
	}
	var index uint64
	if matchIdx >= 0 && matchIdx < len(r.RaftLog.entries) {
		index = r.RaftLog.entries[matchIdx].Index
	} else if matchIdx >= len(r.RaftLog.entries) && len(r.RaftLog.entries) > 0 {
		index = r.RaftLog.entries[len(r.RaftLog.entries)-1].Index
	}
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse,
		To: m.From, From: m.To, Reject: reject, Term: r.Term, LogTerm: match.Term,
		Index: index,
	})
}

func (r *Raft) handleVoteMsg(m pb.Message) {
	reject := true
	if m.Term < r.Term {
		m.Term = r.Term
	}
	var lastEntity pb.Entry
	if l := len(r.RaftLog.entries); l > 0 {
		lastEntity = r.RaftLog.entries[l-1]
	}
	if (r.Vote == 0 || r.Vote == m.From || m.Term > r.Term) &&
		(lastEntity.Term < m.LogTerm ||
			(lastEntity.Term == m.LogTerm && lastEntity.Index <= m.Index)) {
		reject = false
		r.Vote = m.From
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    m.Term,
		Reject:  reject,
	})
}

func (r *Raft) sendCommit() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppend, From: r.id, To: id, Term: r.Term, Commit: r.RaftLog.committed,
			Index: r.RaftLog.LastIndex()})
	}
}

func (r *Raft) processMsgHup() {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		r.RaftLog.committed = r.RaftLog.LastIndex()
	} else {
		for p := range r.Prs {
			r.sendAppend(p)
		}
	}
}

func (r *Raft) handleProposeMsg(m pb.Message) {
	entries := lo.Map(m.Entries, func(e *pb.Entry, i int) pb.Entry {
		idx := r.RaftLog.LastIndex() + uint64(i) + 1
		r.RaftLog.proposeResp[idx] += 1
		return pb.Entry{
			EntryType: e.EntryType,
			Term:      r.Term,
			Index:     idx,
			Data:      e.Data,
		}
	})
	r.RaftLog.entries = append(r.RaftLog.entries, entries...)
	if len(r.Prs) == 1 {
		//r.RaftLog.committed = lo.Min([]uint64{r.RaftLog.LastIndex(), m.Commit})
		r.RaftLog.committed = r.RaftLog.LastIndex()
		return
	}
	for p := range r.Prs {
		r.sendAppend(p)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
