package raftkv

import (
	"encoding/gob"
	"log"
	"src/labrpc"
	"src/raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0
const maxWaitTimeout = time.Duration(6000) * time.Millisecond // 最大等待log_commited延迟  6s

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Pid int64
	Uid int64

	Style string
	Key   string
	Value string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	db sync.Map // kv-store内存中的状态用hash table表示

	/** key存在，说明后台有正在运行的对应worker，正在搬运msg to log_applied_XX_chan
	由于每个pid只允许存在一个搬运worker，使用map不用加锁来控制并发情况 **/
	cacheWorkerKillChan map[int64]chan bool
	cacheQueueByPid     map[int64][]raft.ApplyMsg

	logAppliedStartChan map[int]chan raft.ApplyMsg
	logAppliedKillChan  map[int]chan bool
	logAppliedDoneChan  map[int]chan bool
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	opIn := Op{
		Pid:   args.Pid,
		Style: "Get",
		Key:   args.Key,
	}

	logId, logTerm, ok := kv.rf.Start(opIn)
	DPrintf("kvs[%v] [start] Get %v. (Index:%v-Term:%v-ok:%v)\n", kv.me, opIn,
		logId, logTerm, ok)

	//Case1: add log false
	if !ok {
		reply.WrongLeader = true
		reply.Err = "not leader! fail to add log to raft"
		return
	}

	//Case2: add log success 等待raft commited
	kv.logAppliedStartChan[logId] = make(chan raft.ApplyMsg)
	kv.logAppliedKillChan[logId] = make(chan bool)
	kv.logAppliedDoneChan[logId] = make(chan bool)

	defer func() {
		kv.logAppliedDoneChan[logId] <- true
		delete(kv.logAppliedKillChan, logId)
	}()

	select {
	case msg := <-kv.logAppliedStartChan[logId]:

		// 成功拿到commit后的结果
		opOut := msg.Command.(Op)

		if opIn.Key == opOut.Key {
			value, ok := kv.db.Load(opOut.Key)

			if ok {
				reply.WrongLeader = false // success return with value
				reply.Value = value.(string)
				return
			} else {
				reply.WrongLeader = false
				reply.Err = "key not in kv-store"
				return
			}
		}
	case <-kv.logAppliedKillChan[logId]:
		reply.WrongLeader = true
		reply.Err = "not leader! state is follower now"
		return

	case <-time.After(maxWaitTimeout): // 超时
		reply.WrongLeader = false
		reply.Err = "max wait timeout"
		return
	}

	reply.WrongLeader = false
	reply.Err = "opIn.Key != opOut.Key"
	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	opIn := Op{
		Pid:   args.Pid,
		Style: args.Op,
		Key:   args.Key,
		Value: args.Value,
	}

	logId, logTerm, ok := kv.rf.Start(opIn)

	DPrintf("kvs[%v] [start] Op_put_append %v. (Index:%v-Term:%v-ok:%v)\n", kv.me, opIn,
		logId, logTerm, ok)

	//Case1: add log false
	if !ok {
		reply.WrongLeader = true
		DPrintf("kvs[%v] [False1] Op_put_append %v\n", kv.me, opIn)
		reply.Err = "not leader! fail to add log to raft"
		return
	}

	//Case2: add log success 等待raft commited
	kv.logAppliedStartChan[logId] = make(chan raft.ApplyMsg)
	kv.logAppliedKillChan[logId] = make(chan bool)
	kv.logAppliedDoneChan[logId] = make(chan bool)

	defer func() {
		kv.logAppliedDoneChan[logId] <- true
		DPrintf("kvs[%v] [Done signal] log applied done\n", kv.me)
		delete(kv.logAppliedKillChan, logId)
	}()

	select {
	case msg := <-kv.logAppliedStartChan[logId]:

		// 成功拿到commit后的结果
		opOut := msg.Command.(Op)

		if opIn.Key == opOut.Key {
			if opOut.Style == "Put" {
				kv.db.Store(opOut.Key, opOut.Value)

			} else if opOut.Style == "Append" {
				oldValue, ok := kv.db.Load(opOut.Key)
				if !ok { // key not in map
					oldValue = ""
				}
				kv.db.Store(opOut.Key, oldValue.(string)+opOut.Value)
			}

			reply.WrongLeader = false
			reply.Err = ""
			DPrintf("kvs[%v] [Sucess] Op_put_append[%v] reply[%v]\n", kv.me, opIn, reply)
		} else {
			reply.WrongLeader = false
			reply.Err = "opIn.Key != opOut.Key, retry"
			DPrintf("kvs[%v] [False4] Op_put_append %v\n", kv.me, opIn)
		}

		// kv.logAppliedDoneChan[logId] <- true
		// DPrintf("kvs[%v] [Done signal] log applied done\n", kv.me)
		return

	case <-kv.logAppliedKillChan[logId]:
		reply.WrongLeader = true
		reply.Err = "not leader! state is follower now"
		DPrintf("kvs[%v] [False2] Op_put_append %v\n", kv.me, opIn)
		return

	case <-time.After(maxWaitTimeout): // 超时
		reply.WrongLeader = false
		reply.Err = "max wait timeout, retry"
		DPrintf("kvs[%v] [False3] Op_put_append %v\n", kv.me, opIn)
		return
	}
}

// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *RaftKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *RaftKV) serverLoop() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			if msg.Index == 0 {
				continue
			}

			if _, isLeader := kv.rf.GetState(); isLeader {
				DPrintf("\t\tLeader[%v] is alive\n", kv.me)
				kv.leaderActivity(msg)

			} else {
				DPrintf("\t\tFollower[%v] is alive\n", kv.me)
				kv.followerActivity(msg)
			}
		default:
			// 周期性清理一些 msg cache
		}
	}
}

/*
*

	(!!重要!!) sequential consistency 保证一个pid提交的东西，按照先后顺序执行
	不同pid间可以并发处理，在返回客户端结果前，不能超期处理同个id下的后续任务

*
*/
func (kv *RaftKV) leaderActivity(msg raft.ApplyMsg) {
	DPrintf("kvs[%v] leader get applied Message[%v]\n", kv.me, msg.Index)

	// fan out pattern
	pid := msg.Command.(Op).Pid // 这个id也可以表示成事务的id，clerk_id. 维持一个process内部的op处理上的先后关系

	kv.mu.Lock()
	val, ok := kv.cacheQueueByPid[pid]
	if ok {
		kv.cacheQueueByPid[pid] = append(val, msg)
	} else {
		kv.cacheQueueByPid[pid] = []raft.ApplyMsg{msg}
	}
	DPrintf("kvs[%v] leader cache Msg[%v]\n", kv.me, kv.cacheQueueByPid[pid])

	// 没有work去搬运缓存时，启动新的worker线程，并传入线程关闭chan
	_, ok2 := kv.cacheWorkerKillChan[pid]
	if !ok2 {
		killChan := make(chan bool)
		kv.cacheWorkerKillChan[pid] = killChan
		go kv.cacheWorker(pid, killChan)
	}
	kv.mu.Unlock()

	return
}

func (kv *RaftKV) clean() {
	for _, killChan := range kv.logAppliedKillChan {
		killChan <- true
	}
	for _, killChan := range kv.cacheWorkerKillChan {
		killChan <- true
	}

	kv.cacheWorkerKillChan = make(map[int64]chan bool)
	kv.cacheQueueByPid = make(map[int64][]raft.ApplyMsg)
	kv.logAppliedStartChan = make(map[int]chan raft.ApplyMsg)
	kv.logAppliedKillChan = make(map[int]chan bool)
	kv.logAppliedDoneChan = make(map[int]chan bool)
}

func (kv *RaftKV) followerActivity(msg raft.ApplyMsg) {
	// leader变为follower的特殊情况, clean wrong waitting goroutinue
	// kv.clean()

	// 一个一个处理msg就行
	op := msg.Command.(Op)
	if op.Style == "Get" {
		//read op忽略

	} else if op.Style == "Put" { // put append用于更新状态机
		kv.db.Store(op.Key, op.Value)

	} else if op.Style == "Append" {
		oldValue, ok := kv.db.Load(op.Key)
		if !ok { // key not in map
			oldValue = ""
		}
		kv.db.Store(op.Key, oldValue.(string)+op.Value)
	}

	return
}

// send Cached Msg(op from same pid) to index_Channel one by one
func (kv *RaftKV) cacheWorker(pid int64, killChan chan bool) {
	// todo 设置最大运行时间，超过ns没有新的cache，关闭当前线程，并清理cacheWorkerKillChan
	defer func() {
		delete(kv.cacheWorkerKillChan, pid)
	}()

	DPrintf("kvs[%v] cache worker[%v] \n", kv.me, pid)

	for {
		kv.mu.Lock() //todo 这个锁把整个cacheQueue给锁上了，粒度太粗，竞争关系太强。
		v, ok := kv.cacheQueueByPid[pid]
		var msg raft.ApplyMsg
		succ := false
		if ok && len(v) > 0 {
			msg = v[0]
			kv.cacheQueueByPid[pid] = v[1:] // (!!注意!!) 踩坑，go不支持穿引用
			succ = true
		}
		kv.mu.Unlock()

		if succ {
			kv.logAppliedStartChan[msg.Index] <- msg
			select {
			case <-killChan:
				break
			case <-kv.logAppliedDoneChan[msg.Index]: // kv-store内部完成处理，可以安全处理pid内的下一个Op
			}
		} else {
			select {
			case <-killChan:
				break
			default:
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := &RaftKV{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raft.ApplyMsg),

		cacheWorkerKillChan: make(map[int64]chan bool),
		cacheQueueByPid:     make(map[int64][]raft.ApplyMsg),

		logAppliedStartChan: make(map[int]chan raft.ApplyMsg),
		logAppliedKillChan:  make(map[int]chan bool),
		logAppliedDoneChan:  make(map[int]chan bool),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// kv.db = make(map[string]string)

	DPrintf("start kv server %v\n", kv.me)
	go kv.serverLoop() // main loop logic

	return kv
}
