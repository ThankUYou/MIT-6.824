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
	"6.824/labgob"
	"bytes"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 节点的角色
type Status int
type LogEntries []LogEntry

// 节点角色枚举
const (
	Follower  Status = iota // 跟随者
	Candidate               // 竞争者
	Leader                  // 领导者
)

// 投票响应的类型
type VotedStatus int

const (
	Clash    VotedStatus = iota // 节点clash
	Outdated                    // 竞选者过时（任期落后或者日落后）
	Voted                       // 该节点的票已经投出去了
	Normal                      // 投票，竞选者获得选票
)

type AppendEntriesStatus int

const (
	Killed              AppendEntriesStatus = iota // 节点clash
	Expire                                         // 领导者任期落后
	LogMismatch                                    // 日志不匹配
	Applied                                        // Leader 日志落后, Follower 已经应用
	AppendEntriesNormal                            // 正常

)

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 所有服务器上的持久性状态 (在响应 RPC 请求之前，已经更新到了稳定的存储设备)

	currentTerm int        // 服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	votedFor    int        // 当前任期内收到选票的 candidateId ，如果没有投给任何候选人则为空
	voteNum     int        // 当前任期内获得的票数
	logs        LogEntries //日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）

	// 所有服务器上的易失性状态

	commitIndex int // 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	// 领导人（服务器）上的易失性状态 (选举后已经重新初始化)

	nextIndex  []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	// Others
	status     Status    // 当前节点状态
	votedTimer time.Time // 选举重置时间

	applyChan chan ApplyMsg // 用来写入通道

	lastIncludeIndex int // 快照中包含的最后日志条目的索引值
	lastIncludeTerm  int // 快照中包含的最后日志条目的任期号
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.status == Leader
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	return data
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

type InstallSnapshotArgs struct {
	Term             int    // leader's term
	LeaderId         int    // so follower can redirect clients
	LastIncludeIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludeTerm  int    // term of lastIncludedIndex
	Data             []byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

func (rf *Raft) leaderSendSnapShot(server int) {
	rf.mu.Lock()
	Debug(dSnap, "S%d -> S%d Sending installing snapshot request at T%d.", rf.me, server, rf.currentTerm)
	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludeIndex,
		rf.lastIncludeTerm,
		rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()

	res := rf.sendInstallSnapshot(server, &args, &reply)

	if res {
		rf.mu.Lock()
		if rf.status != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		// 如果返回的term比自己大说明自身数据已经不合适了
		if reply.Term > rf.currentTerm {
			Debug(dSnap, "S%d Current Term lower, change to Follower (%d < %d)",
				rf.me, reply.Term, rf.currentTerm)
			rf.status = Follower
			rf.votedFor = -1
			rf.voteNum = 0
			rf.persist()
			rf.votedTimer = time.Now()
			rf.mu.Unlock()
			return
		}
		// reset matchIndex nextIndex
		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}

// InstallSnapShot RPC Handler
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	Debug(dSnap, "S%d <- S%d Received install snapshot request at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	if rf.currentTerm > args.Term {
		Debug(dSnap, "S%d Term is lower, rejecting install snapshot request. (%d < %d)", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	rf.status = Follower
	rf.votedFor = -1
	rf.voteNum = 0
	rf.persist()
	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.votedTimer = time.Now()

	if rf.lastIncludeIndex >= args.LastIncludeIndex {
		Debug(dSnap, "S%d A newer snapshot already exists, rejecting install snapshot request. (%d <= %d)",
			rf.me, args.LastIncludeIndex, rf.lastIncludeIndex)
		rf.mu.Unlock()
		return
	}

	// 将快照后的logs切割，快照前的直接applied
	index := args.LastIncludeIndex
	tempLog := make([]LogEntry, 0)
	tempLog = append(tempLog, LogEntry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		restoredLogEntry, _ := rf.restoreLog(i)
		tempLog = append(tempLog, restoredLogEntry)
	}
	rf.logs = tempLog
	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.lastIncludeIndex = args.LastIncludeIndex
	Debug(dSnap, "S%d reset lastIncludeTerm:%d, lastIncludeIndex:%d", rf.me, rf.lastIncludeTerm, args.LastIncludeIndex)
	if index > rf.commitIndex {
		Debug(dSnap, "S%d reset commitIndex:%d", rf.me, index)
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		Debug(dSnap, "S%d reset lastApplied:%d", rf.me, index)
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}
	Debug(dSnap, "S%d send snap msg to applyChan")
	rf.mu.Unlock()

	rf.applyChan <- msg

}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
// 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d Snapshotting through index %d.", rf.me, index)
	// 如果下标大于自身的提交，说明没被提交不能安装快照，如果自身快照点大于index说明不需要安装
	if rf.lastIncludeIndex >= index {
		Debug(dSnap, "S%d Snapshot already applied to persistent storage. (%d >= %d)", rf.me, rf.lastIncludeIndex, index)
		return
	}
	if rf.commitIndex < index {
		Debug(dWarn, "S%d Cannot snapshot uncommitted log entries, discard the call. (%d < %d)", rf.me, rf.commitIndex, index)
		return
	}
	// 更新快照日志
	sLogs := make([]LogEntry, 0)
	sLogs = append(sLogs, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		restoredLogEntry, _ := rf.restoreLog(i)
		sLogs = append(sLogs, restoredLogEntry)
	}

	// 更新快照下标/任期
	if index == rf.getLastIndex()+1 {
		rf.lastIncludeTerm = rf.getLastTerm()
	} else {
		rf.lastIncludeTerm = rf.restoreLogTerm(index)
	}

	rf.lastIncludeIndex = index
	Debug(dSnap, "S%d reset lastIncludeTerm:%d, lastIncludeIndex:%d", rf.me, rf.lastIncludeTerm, index)
	rf.logs = sLogs

	// reset commitIndex lastApplied
	if index > rf.commitIndex {
		Debug(dSnap, "S%d reset commitIndex:%d", rf.me, index)
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		Debug(dSnap, "S%d reset lastApplied:%d", rf.me, index)
		rf.lastApplied = index
	}

	// 持久化快照信息
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人所在的任期
	CandidateId  int // 请求选票的候选人的 ID
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

func (rf *Raft) sendElection() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 开启协程对各个节点发起选举
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				rf.getLastIndex(),
				rf.getLastTerm(),
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			res := rf.sendRequestVote(server, &args, &reply)

			if res {
				rf.mu.Lock()
				Debug(dVote, "S%d <- S%d Received request vote reply at T%d.", rf.me, server, rf.currentTerm)
				// 判断自身是否还是竞选者，且任期不冲突
				if rf.status != Candidate || args.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				// 返回者的任期大于args（网络分区原因)进行返回
				if reply.Term > args.Term {
					Debug(dVote, "S%d Term is lower, Candidate change to Follower. (%d < %d)", rf.me, args.Term, rf.currentTerm)
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
					rf.status = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.mu.Unlock()
					return
				}

				// 返回结果正确判断是否大于一半节点同意
				if reply.VoteGranted && rf.currentTerm == args.Term {
					rf.voteNum += 1
					Debug(dVote, "S%d <- S%d Get a yes vote at T%d.", rf.me, server, rf.currentTerm)
					if rf.voteNum >= len(rf.peers)/2+1 {
						Debug(dLeader, "S%d Received majority votes at T%d. Become leader.", rf.me, rf.currentTerm)
						rf.status = Leader
						rf.votedFor = -1
						rf.voteNum = 0
						rf.persist()

						// 初始话 nextIndex[] matchIndex[]
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.getLastIndex() + 1
							rf.matchIndex[i] = 0
						}
						rf.matchIndex[rf.me] = rf.getLastIndex()

						Debug(dTimer, "S%d reset voted timer", rf.me)
						rf.votedTimer = time.Now()
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					return
				}

				rf.mu.Unlock()
				return
			}

		}(i)

	}

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dVote, "S%d <- S%d Received vote request at T%d.", rf.me, args.CandidateId, rf.currentTerm)
	// 由于网络分区或者是节点crash，导致的任期比接收者还小，直接返回
	if args.Term < rf.currentTerm {
		Debug(dVote, "S%d Candidate's Term is lower, rejecting the vote. (%d < %d)", rf.me, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	// 预期的结果:任期大于当前节点，进行重置
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.voteNum = 0
		rf.persist()
	}

	// If votedFor is null or candidateId, and candidate’s logs is at
	// least as up-to-date as receiver’s logs, grant vote

	// if candidate’s logs is at least as up-to-date as receiver’s logs -> 判断日志是否conflict
	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) {
		Debug(dVote, "S%d candidate's logs is not at least as up-to-date as receiver's logs, rejecting the vote", rf.me)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// if votedFor is null or candidateId
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term {
		Debug(dVote, "S%d Already voted for S%d, rejecting the vote.", rf.me, rf.votedFor)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		Debug(dVote, "S%d Granting vote to S%d at T%d, votedFor:%d.", rf.me, args.CandidateId, args.Term, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		Debug(dTimer, "S%d Resetting ELT, wait for next potential election timeout.", rf.me)
		rf.votedTimer = time.Now()
		rf.persist()
		return
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return index, term, !isLeader
	}
	if rf.status != Leader {
		return index, term, !isLeader
	} else {
		index := rf.getLastIndex() + 1
		term := rf.currentTerm
		Debug(dClient, "S%d appped logEntry,index:%d, term:%d, command:%d", rf.me, index, term, command)
		rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
		rf.persist()
		return index, term, isLeader
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

// 选举定时器
func (rf *Raft) electionTicker() {
	for !rf.killed() {
		nowTime := time.Now()
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)

		rf.mu.Lock()

		// 时间过期发起选举
		// 如果 sleep 之后，votedTimer 的时间没有被重置，还是在 nowTime 之前 -> 发起新一轮的选举
		if rf.votedTimer.Before(nowTime) && rf.status != Leader {
			// 转变状态
			rf.status = Candidate
			rf.votedFor = rf.me
			rf.voteNum = 1
			rf.currentTerm += 1
			rf.persist()
			Debug(dTerm, "S%d Follower becomes Candidate and initiates elections, currentTerm:%d", rf.me, rf.currentTerm)
			rf.sendElection()
			rf.votedTimer = time.Now()
		}
		rf.mu.Unlock()
	}
}

// 附加日志定时器
func (rf *Raft) appendTicker() {
	for !rf.killed() {
		time.Sleep(HeartbeatSleep * time.Millisecond)
		rf.mu.Lock()
		if rf.status == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

// 提交日志定时器
func (rf *Raft) committedTicker() {
	// put the committed entry to apply on the status machine
	for !rf.killed() {
		time.Sleep(AppliedSleep * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() && !rf.killed() {
			rf.lastApplied += 1
			log, _ := rf.restoreLog(rf.lastApplied)
			Debug(dCommit, "S%d put the committed entry to apply on the status machine, lastApplied:%d, Command:%d",
				rf.me, rf.lastApplied, log.Command)
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       log.Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyChan <- messages
		}
	}
}

type AppendEntriesArgs struct {
	Term         int        // leader 任期
	LeaderId     int        // 领导人id
	PrevLogIndex int        // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int        // 紧邻新日志条目之前的那个日志条目的任期
	Entries      []LogEntry // 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int        // 领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term        int  // 当前任期，对于领导人而言 它会更新自己的任期
	Success     bool // 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	UpNextIndex int  // 第一个与 XTerm 相同的日志条目的索引。领导者可以使用这个信息来找到从哪里开始重新发送日志。
}

func (rf *Raft) leaderAppendEntries() {

	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		// 开启协程并发的进行日志增量
		go func(server int) {
			rf.mu.Lock()
			if rf.status != Leader {
				rf.mu.Unlock()
				return
			}

			// rf.nextIndex[i]-1 <= lastIncludeIndex -> Follower 的日志小于 Leader 的快照状态，将自己的快照发过去
			if rf.nextIndex[server]-1 < rf.lastIncludeIndex {
				Debug(dLog, "S%d old logs were cleared after the snapshot, send an InstallSnapshot RPC instead", rf.me)
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}

			if rf.getLastIndex() >= rf.nextIndex[server] {
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[server]-rf.lastIncludeIndex:]...)
				args.Entries = entries
			} else {
				args.Entries = []LogEntry{}
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			re := rf.sendAppendEntries(server, &args, &reply)

			if re {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				Debug(dLog, "S%d <- S%d Received send entries reply at T%d.", rf.me, server, rf.currentTerm)
				if rf.status != Leader {
					return
				}
				// 返回的任期已经落后当前任期 -> reply 无效
				if reply.Term < rf.currentTerm {
					Debug(dLog, "S%d Reply Term lower, invalid send entry reply. (%d < %d)",
						rf.me, reply.Term, rf.currentTerm)
					return
				}
				// 返回的任期大于当前任期 -> 当前节点落后 change to Follower
				if reply.Term > rf.currentTerm {
					Debug(dLog, "S%d Current Term lower, change to Follower (%d < %d)",
						rf.me, reply.Term, rf.currentTerm)
					rf.currentTerm = reply.Term
					rf.status = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.votedTimer = time.Now()
					return
				}
				// 当前任期不等于发送请求时的任期(发送之后任期发生改变) -> 放弃该回复
				if rf.currentTerm != args.Term {
					Debug(dWarn, "S%d Term has changed after the append request, send entry reply discarded. "+
						"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
					return
				}

				if reply.Success {
					// 更新 Follower 节点的 commitIndex 和 matchIndex
					rf.commitIndex = rf.lastIncludeIndex
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					// 外层遍历下标是否满足,从最大的索引开始反向遍历
					for index := rf.getLastIndex(); index >= rf.lastIncludeIndex+1; index-- {
						sum := 0
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								sum += 1
								continue
							}
							if rf.matchIndex[i] >= index {
								sum += 1
							}
						}

						// 如果超过半数以上节点的匹配索引满足条件，且当前索引对应的任期与当前节点的任期相同
						if sum >= len(rf.peers)/2+1 && rf.restoreLogTerm(index) == rf.currentTerm {
							rf.commitIndex = index
							Debug(dCommit, "S%d Updated commitIndex at T%d for majority consensus. commitIndex: %d.", rf.me, rf.currentTerm, rf.commitIndex)
							break
						}

					}
				} else {
					// 追加日志发生冲突 -> 进行更新
					Debug(dLog, "S%d <- S%d Inconsistent logs, need retry. reset nextIndex=%d", rf.me, server, reply.UpNextIndex)
					if reply.UpNextIndex != -1 {
						rf.nextIndex[server] = reply.UpNextIndex
					}
				}
			}

		}(index)

	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// gid := getGID()
	// beforeLock(gid, "AppendEntries", rf.me)
	// startGetLockTime := time.Now()
	rf.mu.Lock()
	// afterLock(gid, "AppendEntries", startGetLockTime, rf.me)
	// startHoldLockTime := time.Now()
	defer func() {
		// afterUnlock(gid, "AppendEntries", startHoldLockTime, rf.me)
		rf.mu.Unlock()
	}()
	if len(args.Entries) == 0 {
		Debug(dLog2, "S%d <- S%d Received heartbeat at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	} else {
		Debug(dLog2, "S%d <- S%d Received append entries at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	}
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		Debug(dLog2, "S%d Term is lower, rejecting append request. (%d < %d)", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = -1
		return
	}
	// 重置 Follower 节点的状态
	reply.Success = true
	reply.Term = args.Term
	reply.UpNextIndex = -1

	rf.status = Follower
	rf.currentTerm = args.Term
	rf.voteNum = 0
	rf.persist()
	rf.votedTimer = time.Now()

	// 如果自身的快照索引比请求中的 PrevLogIndex 还大，说明存在冲突，返回冲突的下一个索引（用于更新 Leader 的 nextIndex）
	if rf.lastIncludeIndex > args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex() + 1
		return
	}

	// 如果自身的最后日志索引小于请求中的 PrevLogIndex，说明 Follower 缺失日志，返回自身的最后索引
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex()
		return
	} else {
		// 如果在 prevLogIndex 处的日志条目的 term 与 prevLogTerm 不匹配，那么回复 false (§5.3)
		argsPrevLogIndexTerm := rf.restoreLogTerm(args.PrevLogIndex)
		if argsPrevLogIndexTerm != args.PrevLogTerm {
			reply.Success = false
			// the follower can include the term of the conflicting entry and the first index it stores for that term.
			// With this information, the leader can decrement nextIndex to bypass all of the conflicting entries in that term;
			// one AppendEntries RPC will be required for each term with conflicting entries, rather than one RPC per entry.
			reply.UpNextIndex = rf.getMinIndexInOneTerm(argsPrevLogIndexTerm, args.PrevLogIndex)
			Debug(dLog2, "S%d The term of the log entry at prevLogIndex does not match prevLogTerm(%d != %d), reset UpNextIndex: %d",
				rf.me, argsPrevLogIndexTerm, args.PrevLogTerm, reply.UpNextIndex)
			return
		}
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	// 进行日志的截取
	rf.logs = append(rf.logs[:args.PrevLogIndex+1-rf.lastIncludeIndex], args.Entries...)
	rf.persist()

	//update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if len(args.Entries) == 0 {
		Debug(dLog, "S%d -> S%d Leader send heartbeat", rf.me, server)
	} else {
		Debug(dLog, "S%d -> S%d Leader send entries, PrevLogIndex:%d, PrevLogTerm:%d",
			rf.me, server, args.PrevLogIndex, args.PrevLogTerm)
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()

	rf.status = Follower
	rf.currentTerm = 0
	rf.voteNum = 0
	rf.votedFor = -1

	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0

	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{})
	rf.applyChan = applyCh
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 同步快照信息
	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
	}
	// start ticker goroutine to start elections
	//go rf.ticker()
	go rf.electionTicker()

	go rf.appendTicker()

	go rf.committedTicker()

	return rf
}
