package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	leaderId int   // 可见的合法leader
	pid      int64 // clerk process Id
	uid      int64 // Unified Id
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// You'll have to add code here.
	ck := &Clerk{
		servers: servers,

		pid:      nrand(),
		uid:      0,
		leaderId: 0,
	}
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	cuid := atomic.AddInt64(&ck.uid, 1)

	args := GetArgs{
		Pid: ck.pid,
		Uid: cuid,
		Key: key,
	}

	// 客户端可以一直hold吗？
	i := 0
	for {
		lid := (ck.leaderId + i) % len(ck.servers)
		DPrintf("\nClerk[%v] Uid[%v] Get(%v) ->S%v\n", ck.pid, cuid, key, lid)
		reply := GetReply{}
		ok := ck.servers[lid].Call("KVServer.Get", &args, &reply)
		DPrintf("Clerk[%v] Uid[%v] Get(%v) reply_ok(%v), reply(%v)\n", ck.pid, cuid, key, ok, reply)

		if ok {
			if reply.Err == OK {
				// 操作执行成功
				ck.leaderId = lid
				DPrintf("Clerk[%v] Uid[%v] Get(%v) success, reply_value[%v] 结束循环\n", ck.pid, cuid, key, reply.Value)
				return reply.Value
			}

			if reply.Err == ErrNoKey {
				return ""
			}

			if reply.Err == ErrWrongLeader {
				i++
				if i == len(ck.servers) {
					i = 0
					<-time.After(time.Duration(500) * time.Millisecond)
				}
				continue // try next server node
			}

			// 其他error情况呢，比如超时、raft内部同步错误， 同样需要retry（但是不需要更新node id）

		}

	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	cuid := atomic.AddInt64(&ck.uid, 1)

	args := PutAppendArgs{Pid: ck.pid,
		Uid:   cuid,
		Key:   key,
		Value: value,
		Op:    op}

	// 没接收成功就一直发送
	i := 0
	for {
		lid := (ck.leaderId + i) % len(ck.servers)
		DPrintf("\nClerk[%v] uid[%v] %v  (%v : %v) ->S%v\n",
			ck.pid, cuid, args.Op, key, value, lid)

		reply := PutAppendReply{} // (!!注意!!) 放在for外面容易出奇怪错误
		ok := ck.servers[lid].Call("KVServer.PutAppend", &args, &reply)

		DPrintf("Clerk[%v] uid[%v] %v reply ok(%v), reply(%v)\n", ck.pid, cuid, args.Op, ok, reply)
		if ok {
			if reply.Err == OK {
				// 操作执行成功
				ck.leaderId = lid
				DPrintf("Clerk[%v] uid[%v] %v (%v : %v) success, 请求结束\n", ck.pid, cuid, args.Op, key, value)
				return
			}

			if reply.Err == ErrWrongLeader {
				i++
				if i == len(ck.servers) {
					i = 0
					<-time.After(time.Duration(500) * time.Millisecond)
				}
				continue // try next server node
			}
			// 其他error情况呢，比如超时、raft内部同步错误， 同样需要retry（但是不需要更新node id）
		}

	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
