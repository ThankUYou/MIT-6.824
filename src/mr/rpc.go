package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskArgs struct {
}

type Task struct {
	TaskId     int       // 任务唯一 id
	TaskType   TaskType  // 任务类型 -> map/reduce
	FileSlice  []string  // 输入的文件
	ReducerNum int       // reducer 数量
	State      TaskState // 任务状态
	StartTime  time.Time // 任务开始时间
}

// TaskType 枚举
type TaskType int

const (
	MapTask     TaskType = iota // Map
	ReduceTask                  // Reduce
	WaitingTask                 // 任务全部被分发完了
	ExitTask                    // finish
)

// TaskState 枚举
type TaskState int

const (
	Working TaskState = iota // 工作中
	Waiting                  // 等待执行
	Done                     // 已经完成
)

// Phase 程序所处阶段的枚举
type Phase int

const (
	MapPhase    Phase = iota // map 阶段
	ReducePhase              // reduce 阶段
	AllDone                  // 已完成
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
