package chandy_lamport

import (
	"fmt"
	"log"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE

	snapshotStates  map[int]*SnapshotState   // key= snapshotid
	inchanCompleted map[int]*map[string]bool // 记录每条inbound是否get到Marker, key= snapshotid

}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		make(map[int]*SnapshotState),   // key = snapshotId
		make(map[int]*map[string]bool), // key = snapshotId
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME
	if debug {
		fmt.Printf("[sgd-log][HandlePacket]src:%v,desc:%v.\n", src, server.Id)
		server.debug("begin handlepacket")
	}

	switch message := message.(type) {
	case TokenMessage:
		numTokens := message.numTokens
		server.Tokens += numTokens

		for snapid, inchancompl := range server.inchanCompleted {
			snapstate := server.snapshotStates[snapid]
			for fromId, markerGetted := range *inchancompl {
				if src == fromId && !markerGetted {
					curCacheMsg := SnapshotMessage{src, server.Id, message}
					snapstate.messages = append(snapstate.messages, &curCacheMsg)
				}
			}
		}

	case MarkerMessage:
		snapshotId := message.snapshotId
		_, haveSaved := server.snapshotStates[snapshotId]

		if !haveSaved { //没有local state，save it and init inchan{}
			// server.fisrtStateSave(snapshotId)
			server.StartSnapshot(snapshotId)
		}

		(*server.inchanCompleted[snapshotId])[src] = true // inchannel state： save completed

		if debug {
			fmt.Printf("[sgd-log][MarkerMessage]src:%v,desc:%v.\n", src, server.Id)
		}

	default:
		log.Fatal("Error unknown message: ", message)

	}
	server.debug("end handlepacket")

	// 遍历每个snapshotID，如果inchan全部完成，则通知sim local process的该快照已完成。
	clearid := make([]int, 0)
	for snapid, inchancompl := range server.inchanCompleted {
		allDone := true

		for _, markerGetted := range *inchancompl {
			if !markerGetted {
				allDone = false
				break
			}
		}

		if allDone {
			server.sim.NotifySnapshotComplete(server.Id, snapid)
			// 需要删除对应的数据结构
			clearid = append(clearid, snapid)
		}
	}
	// clear
	for _, id := range clearid {
		delete(server.inchanCompleted, id)
	}

}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	// save state of current process and nit inchan {}
	server.fisrtStateSave(snapshotId)

	// 对所有outboundLinks发送MarkerMessage
	server.SendToNeighbors(MarkerMessage{snapshotId})
}

func (server *Server) fisrtStateSave(snapshotId int) {
	// 初始化snapshotStates
	server.snapshotStates[snapshotId] = &SnapshotState{snapshotId,
		map[string]int{server.Id: server.Tokens},
		make([]*SnapshotMessage, 0), //空的channel
	}

	// 初始化 inchanCompleted
	markerFlags := make(map[string]bool)
	for _, serverId := range getSortedKeys(server.inboundLinks) {
		link := server.inboundLinks[serverId]
		markerFlags[link.src] = false
	}
	server.inchanCompleted[snapshotId] = &markerFlags

}

// // State recorded during the snapshot process
// type SnapshotState struct {
// 	id       int
// 	tokens   map[string]int // key = server ID, value = num tokens
// 	messages []*SnapshotMessage
// }

// A message recorded during the snapshot process
// type SnapshotMessage struct {
// 	src     string
// 	dest    string
// 	message interface{}
// }

func (server *Server) debug(prefix string) {
	if debug == false {
		return
	}
	ss := "[sgd-log]"
	ss += "[" + prefix + "]"
	ss += fmt.Sprintf("\n\tserver id(%v)", server.Id)
	for snapid, v := range server.inchanCompleted {
		ss += fmt.Sprintf("\n\t[in-channel]snapshotId{%v}, ", snapid)
		for inchan, flag := range *v {
			ss += fmt.Sprintf("[ch:%v, mark:%v], ", inchan, flag)
		}

	}
	for snapid, state := range server.snapshotStates {
		ss += fmt.Sprintf("\n\t[snapshotStates]snapshotId-%v, tokens{%v}, message_len:%v", snapid, state.tokens, len(state.messages))
		ss += "{"

		for _, msg := range state.messages {
			tmsg, ok := msg.message.(TokenMessage)
			if ok {
				ss += fmt.Sprintf("%v->%v (%v); ", msg.src, msg.dest, tmsg.numTokens)
			}
		}
		ss += "}\n"
	}

	fmt.Println(ss)

}
