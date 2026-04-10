package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rememorio/MIT-6.5840/labgob"
	"github.com/Rememorio/MIT-6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

type LogEntry struct {
	Command any
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state (3C)
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state
	commitIndex int
	lastApplied int

	// Leader volatile state
	nextIndex  []int
	matchIndex []int

	// Election state
	state         int
	electionTimer time.Time

	// Apply channel
	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// Snapshot state (3D)
	lastIncludedIndex int
	lastIncludedTerm  int

	// Pending snapshot to be applied
	pendingSnapshot      []byte
	pendingSnapshotTerm  int
	pendingSnapshotIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// ---------- Log indexing helpers (3D) ----------

// realIndex converts a global log index to the slice index
func (rf *Raft) realIndex(globalIdx int) int {
	return globalIdx - rf.lastIncludedIndex
}

// globalIndex converts a slice index to global log index
func (rf *Raft) globalIndex(sliceIdx int) int {
	return sliceIdx + rf.lastIncludedIndex
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) logTerm(globalIdx int) int {
	if globalIdx == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.log[rf.realIndex(globalIdx)].Term
}

// ---------- Persistence (3C) ----------

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

// ---------- Snapshot (3D) ----------

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	// trim log
	newLog := make([]LogEntry, len(rf.log[rf.realIndex(index):]))
	copy(newLog, rf.log[rf.realIndex(index):])
	rf.lastIncludedTerm = rf.logTerm(index)
	rf.lastIncludedIndex = index
	rf.log = newLog
	// dummy entry at index 0
	rf.log[0] = LogEntry{Term: rf.lastIncludedTerm}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	rf.persister.Save(w.Bytes(), snapshot)
}

// ---------- RequestVote RPC (3A, 3B) ----------

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm

	// Check if we can vote for this candidate
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// Election restriction: candidate's log must be at least as up-to-date
		lastTerm := rf.lastLogTerm()
		lastIndex := rf.lastLogIndex()
		if args.LastLogTerm > lastTerm ||
			(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex) {
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.resetElectionTimer()
			reply.VoteGranted = true
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// ---------- AppendEntries RPC (3A, 3B) ----------

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// Fast backup (3C)
	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// Log consistency check
	if args.PrevLogIndex < rf.lastIncludedIndex {
		// PrevLogIndex is within the snapshot - this means we already have these entries
		// We need to trim the entries that overlap with our snapshot
		overlap := rf.lastIncludedIndex - args.PrevLogIndex
		if overlap >= len(args.Entries) {
			// all entries are already in our snapshot
			reply.Success = true
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				if rf.lastLogIndex() < rf.commitIndex {
					rf.commitIndex = rf.lastLogIndex()
				}
				rf.applyCond.Signal()
			}
			return
		}
		args.Entries = args.Entries[overlap:]
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = rf.lastIncludedTerm
	}

	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.XLen = rf.lastLogIndex() + 1
		reply.XTerm = -1
		reply.XIndex = -1
		return
	}

	if rf.logTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.XTerm = rf.logTerm(args.PrevLogIndex)
		// Find first index of XTerm
		reply.XIndex = args.PrevLogIndex
		for reply.XIndex > rf.lastIncludedIndex+1 && rf.logTerm(reply.XIndex-1) == reply.XTerm {
			reply.XIndex--
		}
		reply.XLen = rf.lastLogIndex() + 1
		return
	}

	// Append new entries
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx > rf.lastLogIndex() {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		} else if rf.logTerm(idx) != entry.Term {
			rf.log = rf.log[:rf.realIndex(idx)]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	rf.persist()

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if lastNewIndex < rf.commitIndex {
			rf.commitIndex = lastNewIndex
		}
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// ---------- InstallSnapshot RPC (3D) ----------

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// Don't apply snapshot if we've already applied past it
	shouldApplySnapshot := args.LastIncludedIndex > rf.lastApplied

	// If existing log entry has same index and term as snapshot's last included entry,
	// retain log entries following it
	if args.LastIncludedIndex <= rf.lastLogIndex() &&
		rf.logTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		newLog := make([]LogEntry, len(rf.log[rf.realIndex(args.LastIncludedIndex):]))
		copy(newLog, rf.log[rf.realIndex(args.LastIncludedIndex):])
		rf.log = newLog
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.log[0] = LogEntry{Term: rf.lastIncludedTerm}

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	rf.persister.Save(w.Bytes(), args.Data)

	// Queue snapshot for applier goroutine only if it advances past lastApplied
	if shouldApplySnapshot {
		rf.pendingSnapshot = args.Data
		rf.pendingSnapshotTerm = args.LastIncludedTerm
		rf.pendingSnapshotIndex = args.LastIncludedIndex
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// ---------- Start (3B) ----------

func (rf *Raft) Start(command any) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	entry := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, entry)
	rf.persist()

	index := rf.lastLogIndex()
	term := rf.currentTerm

	// Trigger immediate replication
	rf.broadcastAppendEntries()

	return index, term, true
}

// ---------- Election (3A) ----------

func (rf *Raft) resetElectionTimer() {
	ms := 300 + (rand.Int63() % 300)
	rf.electionTimer = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()

	term := rf.currentTerm
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()
	votes := 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(server, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.currentTerm != term || rf.state != Candidate {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					return
				}

				if reply.VoteGranted {
					votes++
					if votes > len(rf.peers)/2 {
						rf.state = Leader
						// Initialize leader state
						for j := range rf.peers {
							rf.nextIndex[j] = rf.lastLogIndex() + 1
							rf.matchIndex[j] = 0
						}
						rf.broadcastAppendEntries()
					}
				}
			}
		}(i)
	}
}

// ---------- Leader replication ----------

func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			// Need to send snapshot
			go rf.sendSnapshotTo(i)
		} else {
			go rf.sendAppendEntriesTo(i)
		}
	}
}

func (rf *Raft) sendSnapshotTo(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}
	if rf.sendInstallSnapshot(server, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.currentTerm != args.Term || rf.state != Leader {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return
		}

		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}
}

func (rf *Raft) sendAppendEntriesTo(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	// If nextIndex is behind our snapshot, send snapshot instead
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		rf.sendSnapshotTo(server)
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.logTerm(prevLogIndex)

	var entries []LogEntry
	if rf.nextIndex[server] <= rf.lastLogIndex() {
		entries = make([]LogEntry, len(rf.log[rf.realIndex(rf.nextIndex[server]):]))
		copy(entries, rf.log[rf.realIndex(rf.nextIndex[server]):])
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	term := rf.currentTerm
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(server, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.currentTerm != term || rf.state != Leader {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return
		}

		if reply.Success {
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			if newMatchIndex > rf.matchIndex[server] {
				rf.matchIndex[server] = newMatchIndex
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.updateCommitIndex()
		} else {
			// Fast backup (3C)
			if reply.XTerm == -1 {
				// Follower's log is too short
				rf.nextIndex[server] = reply.XLen
			} else {
				// Look for XTerm in leader's log
				found := false
				for idx := rf.lastLogIndex(); idx > rf.lastIncludedIndex; idx-- {
					if rf.logTerm(idx) == reply.XTerm {
						rf.nextIndex[server] = idx + 1
						found = true
						break
					} else if rf.logTerm(idx) < reply.XTerm {
						break
					}
				}
				if !found {
					rf.nextIndex[server] = reply.XIndex
				}
			}
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	for n := rf.lastLogIndex(); n > rf.commitIndex && n > rf.lastIncludedIndex; n-- {
		if rf.logTerm(n) != rf.currentTerm {
			continue
		}
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			break
		}
	}
}

// ---------- Apply goroutine ----------

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && rf.pendingSnapshot == nil {
			rf.applyCond.Wait()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}

		// Check for pending snapshot first - always prioritize snapshot
		if rf.pendingSnapshot != nil {
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.pendingSnapshot,
				SnapshotTerm:  rf.pendingSnapshotTerm,
				SnapshotIndex: rf.pendingSnapshotIndex,
			}
			rf.pendingSnapshot = nil
			if msg.SnapshotIndex > rf.lastApplied {
				rf.lastApplied = msg.SnapshotIndex
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
		}

		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied

		// If lastApplied is before snapshot, skip to snapshot boundary
		if lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
			rf.mu.Unlock()
			continue
		}

		// Build batch of entries to apply
		var msgs []ApplyMsg
		for i := lastApplied + 1; i <= commitIndex && i <= rf.lastLogIndex(); i++ {
			ri := rf.realIndex(i)
			if ri < 1 || ri >= len(rf.log) {
				break
			}
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[ri].Command,
				CommandIndex: i,
			})
		}

		// Update lastApplied before releasing lock to prevent
		// re-applying entries if Snapshot trims log
		if len(msgs) > 0 {
			rf.lastApplied = msgs[len(msgs)-1].CommandIndex
		}
		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	}
}

// ---------- Kill ----------

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.applyCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ---------- Ticker ----------

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		timerExpired := time.Now().After(rf.electionTimer)
		rf.mu.Unlock()

		if state == Leader {
			rf.mu.Lock()
			rf.broadcastAppendEntries()
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		} else {
			if timerExpired {
				rf.mu.Lock()
				rf.startElection()
				rf.mu.Unlock()
			}
			ms := 10 + (rand.Int63() % 20)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}

// ---------- Make ----------

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Initialize state
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}} // dummy entry at index 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = Follower
	rf.resetElectionTimer()

	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Restore lastApplied from snapshot
	if rf.lastIncludedIndex > 0 {
		rf.lastApplied = rf.lastIncludedIndex
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier goroutine
	go rf.applier()

	return rf
}
