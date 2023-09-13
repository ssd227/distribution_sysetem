package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

const maxWaitTimeout = time.Duration(6000) * time.Millisecond // 最大等待log_commited延迟  6s

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type AppliedIndex struct {
	mu      sync.Mutex
	id      int
	resChan map[int]chan OpResult
}

func NewAppliedIndex() *AppliedIndex {
	return &AppliedIndex{
		id:      0,
		resChan: make(map[int]chan OpResult),
	}
}

func (ai *AppliedIndex) setId(index int) {
	ai.mu.Lock()
	defer ai.mu.Unlock()
	ai.id = index
}

func (ai *AppliedIndex) processing(index int, opRes OpResult) {
	ai.mu.Lock()
	defer ai.mu.Unlock()

	resch := ai.resChan[index]
	resch <- opRes
	delete(ai.resChan, index)
}

func (ai *AppliedIndex) waitResult(index int) chan OpResult {
	ai.mu.Lock()
	resch := make(chan OpResult, 1)
	ai.resChan[index] = resch
	ai.mu.Unlock()

	return resch
}

type OpResult struct {
	OK    bool
	Value string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Pid int64
	Uid int64

	Key   string
	Value string
	Type  string
}

func (op Op) String() string {
	return fmt.Sprintf("OP{Pid[%v]-Uid[%v]-Type[%v]-Key[%v]-Value[%v]}",
		op.Pid, op.Uid, op.Type, op.Key, op.Value)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db        sync.Map // kv-store内存中的状态用hash table表示
	appliedId AppliedIndex
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Pid:  args.Pid,
		Uid:  args.Uid,
		Type: OpGet,
		Key:  args.Key,
	}

	logId, logTerm, ok := kv.rf.Start(op)
	DPrintf("kvs[%v] [start] Get %v. (Index:%v-Term:%v-ok:%v)\n", kv.me, op,
		logId, logTerm, ok)

	//Case1: add log false
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	// wait here for operation result
	waitChan := kv.appliedId.waitResult(logId)

	select {
	case opRes := <-waitChan:
		if opRes.OK {
			reply.Err = OK // success return with value
			reply.Value = opRes.Value
			return
		} else {
			reply.Err = ErrNoKey
			return
		}

	case <-time.After(maxWaitTimeout): // 超时
		reply.Err = ErrMaxWaitTimeout
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Pid: args.Pid,
		Uid: args.Uid,

		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
	}

	logId, logTerm, ok := kv.rf.Start(op)

	DPrintf("kvs[%v] [start] Op_put_append %v. (Index:%v-Term:%v-ok:%v)\n", kv.me, op,
		logId, logTerm, ok)

	//Case1: add log false
	if !ok {
		reply.Err = ErrWrongLeader
		DPrintf("kvs[%v] [False1] Op_put_append %v\n", kv.me, op)
		return
	}

	// wait here for operation result
	waitChan := kv.appliedId.waitResult(logId)

	select {
	case opRes := <-waitChan:
		if opRes.OK {
			reply.Err = OK // success return with value
			DPrintf("kvs[%v] [Sucess] Op_put_append[%v] reply[%v]\n", kv.me, op, reply)
			return
		}

	case <-time.After(maxWaitTimeout): // 超时
		reply.Err = ErrMaxWaitTimeout
		DPrintf("kvs[%v] [False2] Op_put_append %v\n", kv.me, op)
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) serverLoop() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandIndex == 0 {
				continue
			}
			// 需要改成生产者消费者模式
			DPrintf("S%v get_message[%v]\n", kv.me, msg)

			kv.processOneMsg(msg)
			kv.leaderActivity(msg)

			//(注意：加上就错，不加就test pass)
			// default:
			// 	// 周期性清理一些 msg cache
			// 	if _, isLeader := kv.rf.GetState(); !isLeader {
			// 		DPrintf("\tFollower[%v] is alive\n", kv.me)
			// 		return
			// 	} else {
			// 		DPrintf("\tLeader[%v] is alive\n", kv.me)
			// 	}
		}
	}
}

func (kv *KVServer) leaderActivity(msg raft.ApplyMsg) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		DPrintf("\t\tFollower[%v] is alive\n", kv.me)
		return
	}

	DPrintf("\t\tLeader[%v] is alive\n", kv.me)
	// 返回当前处理完index后，状态机的状态。然后继续处理下一个命令

	op := msg.Command.(Op)
	if op.Type == OpGet { //OP: Get
		if v, ok := kv.db.Load(op.Key); ok {
			kv.appliedId.processing(msg.CommandIndex, OpResult{OK: ok, Value: v.(string)})
		} else {
			kv.appliedId.processing(msg.CommandIndex, OpResult{OK: ok, Value: ""})
		}
	} else { // OP: PUT or APPEND
		kv.appliedId.processing(msg.CommandIndex, OpResult{OK: true})
	}

}

func (kv *KVServer) processOneMsg(msg raft.ApplyMsg) {
	// 处理msg 忽略OpGet
	op := msg.Command.(Op)
	if op.Type == OpPut { // put append用于更新状态机
		kv.db.Store(op.Key, op.Value)

	} else if op.Type == OpAppend {
		oldValue, ok := kv.db.Load(op.Key)
		if !ok { // key not in map
			oldValue = ""
		}
		kv.db.Store(op.Key, oldValue.(string)+op.Value)
	}

	return
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := &KVServer{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raft.ApplyMsg),
		appliedId:    *NewAppliedIndex(),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// kv.db = make(map[string]string)

	// You may need initialization code here.
	DPrintf("start kv server %v\n", kv.me)
	go kv.serverLoop() // main loop logic

	return kv
}
