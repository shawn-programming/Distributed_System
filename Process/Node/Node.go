package node

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	ms "../../Process/Membership"
	config "../../logSystem/config"
	pk "../Packet"
)

type Leader struct {
	MsListPtr *ms.MsList
	FileList  map[string][]ms.Id
	IdList    map[ms.Id][]string
}

// func LeaderInitialization(MsListPtr *ms.MsList) {
// 	leader := Leader{MsListPtr, make(map[string][]string{})}

// 	for _, member := range MsListPtr.List {
// 		FileList[]
// 	}
// }

/*
Node for a processor
*/

type Node struct {
	// Node's main info
	Id     ms.Id     // node's id
	MsList ms.MsList // node's membership list

	// constant attributes
	VmNum        int    // vm's number
	VmNumStr     string // vm's number in string
	MyService    string // myservice for networking
	TimeOut      int    // time limit for failing
	FailRate     int    // message drop ratio
	IntroducerIP string // Introducer's IP
	IsIntroducer bool   // True if the node is the introducer
	SelfIP       string // node's IP

	MyPortNum   int // node's port number
	DestPortNum int // introducer's port number

	MyPortNumHB   int // heartbeat port
	DestPortNumHB int // heartbeat port

	ServerID        string // node's id in string
	K               int    // gossip's k value
	LocalPath       string // directory that stores local files
	DistributedPath string // directory that stores distributed files
	MaxFail         int    // max number of Fail

	//election
	ElectionInitiatorPtr *string

	// variable attributes
	LocalTime int // local time of the node

	// variable pointers
	LeaderServicePtr    *string      // Leader's Id
	IsLeaderPtr         *bool        // true if node is the leader
	ATAPtr              *bool        // true if heartbeating is all to all else false
	TotalByteSentPtr    *int         // tracks total byte usages
	DistributedFilesPtr *[]string    // list of distributed files
	InputListPtr        *[]ms.MsList // node's InputList (for heartbeating)
	Logger              *log.Logger  // node's main logger
	LoggerPerSec        *log.Logger  // node's heartbeat logger
	LoggerByte          *log.Logger  // node's byte usage tracker

	// leader struct
	LeaderPtr *Leader
}

/*
node.Print()
*/
func (node Node) Print() {
	fmt.Println("TimeOut: ", node.TimeOut)
	node.MsList.Print()
}

/*
node.PrintLog()
RETURN: log
*/
func (node Node) PrintLog() string {
	log := "TimeOut: " + strconv.Itoa(node.TimeOut) + "/n"
	log += node.MsList.PrintLog()
	return log
}

/*
CreateNode(vmNumStr string)
Node Constructor
RETURN: a Node for a processor
*/
func CreateNode(vmNumStr string, IsLeaderPtr, ATAPtr *bool, TotalByteSentPtr *int, InputListPtr *[]ms.MsList, LeaderServicePtr *string, DistributedFilesPtr *[]string, Initiator *string) Node {
	tempNode := Node{}

	failRate, _ := config.FailRate()

	K, _ := config.K() // K value for gossip

	vmNum, _ := strconv.Atoi(vmNumStr) // VM number

	IntroducerIPList, _ := config.IPAddress() // Introducer's IP
	IntroducerIP := IntroducerIPList[1]
	portList, _ := config.Port()     // Port number's list
	portHBList, _ := config.PortHB() // Port number's list

	timeOut, _ := config.TimeOut() // Time Out info
	isIntroducer := vmNum == 1     // True if the proceesor is an introducer, else False
	selfIP := IntroducerIPList[vmNum]

	// for VM test
	myPortNum := portList[0]   // Processor's port number
	destPortNum := portList[0] // Receiver's port number
	myPortNumHB := portHBList[0]
	desPortNumHB := portHBList[0]
	// for local test
	// myPortNum := portList[(vmNum+1)%2]     // Processor's port number
	// destPortNum := portList[vmNum%2]       // Receiver's port number
	// myPortNumHB := portHBList[(vmNum+1)%2] // Processor's port number
	// desPortNumHB := portHBList[vmNum%2]    // Receiver's port number

	myService := selfIP + ":" + strconv.Itoa(myPortNum)                // processor's service for UDP
	serverID := selfIP + "_" + string(time.Now().Format(time.RFC1123)) // Processor's ID

	membership := ms.CreateMembership(serverID, selfIP, 0, 0) // processor's membership list
	var membershipList ms.MsList
	membershipList = membershipList.Add(membership, 0)

	ID := ms.Id{serverID, selfIP}

	// assign attributes to the temp node

	tempNode.Id = ID
	tempNode.MsList = membershipList

	// constant attributes
	tempNode.VmNum = vmNum
	tempNode.VmNumStr = vmNumStr
	tempNode.MyService = myService
	tempNode.TimeOut = timeOut
	tempNode.FailRate = failRate
	tempNode.IntroducerIP = IntroducerIP
	tempNode.IsIntroducer = isIntroducer
	tempNode.SelfIP = selfIP
	tempNode.MyPortNum = myPortNum
	tempNode.MyPortNumHB = myPortNumHB
	tempNode.DestPortNum = destPortNum
	tempNode.DestPortNumHB = desPortNumHB
	tempNode.ServerID = serverID
	tempNode.K = K
	tempNode.LocalPath = "./local_files/"
	tempNode.DistributedPath = "./distributed_files/"
	tempNode.MaxFail = 3

	// variable attributes
	tempNode.LocalTime = 0

	// variable pointers
	tempNode.LeaderServicePtr = LeaderServicePtr
	tempNode.IsLeaderPtr = IsLeaderPtr
	tempNode.ATAPtr = ATAPtr
	tempNode.TotalByteSentPtr = TotalByteSentPtr
	tempNode.InputListPtr = InputListPtr
	tempNode.ElectionInitiatorPtr = Initiator

	// distributred files list
	tempNode.DistributedFilesPtr = DistributedFilesPtr
	files, err := ioutil.ReadDir("./distributed_files")
	checkError(err)
	for _, file := range files {
		(*tempNode.DistributedFilesPtr) = append((*tempNode.DistributedFilesPtr), file.Name())
	}

	// leader pointer
	tempNode.LeaderPtr = nil

	return tempNode
}

/*
AddMember(member ms.Membership)

	Add a member to the node

RETURN:  node with the new member
*/
func (node Node) AddMember(member ms.Membership) Node {
	node.MsList = node.MsList.Add(member, node.LocalTime)
	return node
}

/*
IncrementLocalTime(inputList []ms.MsList)

	Increment local time of the node and update its data

RETURN: updated node
*/
func (node Node) IncrementLocalTime(inputList []ms.MsList) (Node, string) {
	node.LocalTime = node.LocalTime + 1
	var joinLog string
	var failLog string
	var removeLog string

	// wait for 1 sec
	time.Sleep(time.Second)

	// This is necessary for the case when there is no input
	node.MsList = node.MsList.UpdateMsList(ms.MsList{}, node.LocalTime, node.Id)

	var joinLogTotal string
	var removeLogTotal string

	for _, input := range inputList {
		// update newly join members and members' info
		node.MsList, joinLog = node.MsList.CheckMembers(input, node.LocalTime, node.TimeOut)
		joinLogTotal += joinLog
		node.MsList = node.MsList.UpdateMsList(input, node.LocalTime, node.Id)
	}

	// mark fails
	var removeList []ms.Id
	var failList []ms.Id
	node.MsList, failList, removeList, failLog = node.MsList.CheckFails(node.LocalTime, node.TimeOut)

	if *node.IsLeaderPtr {
		// replicate distributed files of members inside the failList

		for _, failed := range failList {
			failedService := failed.IPAddress + ":" + strconv.Itoa(node.DestPortNum)
			fmt.Println(failedService, *node.LeaderServicePtr)
			if failedService == *node.LeaderServicePtr {
				fmt.Println(failedService, "was the leader, starting an election")
				node.initiateElection()
				fmt.Println("NewLeader:", *node.LeaderServicePtr)
			}
			//remove(failed from fileList)
			fileList := node.LeaderPtr.IdList[failed]

			// remove all of its distributed file history from the leader
			for _, file := range fileList {
				IdList := node.LeaderPtr.FileList[file]
				for i, ID := range IdList {
					if ID == failed {
						node.LeaderPtr.FileList[file] = append(IdList[:i], IdList[i+1:]...)
					}
				}
			}

			// make the first alive owner of that file to send a replication to another process
			for _, file := range fileList {
				fileOwners := node.LeaderPtr.FileList[file]
				N := node.MaxFail - len(fileOwners) + 1

				destinations := node.PickReplicas(N, fileOwners)

				from := fileOwners[0]

				Service := from.IPAddress + ":" + strconv.Itoa(node.DestPortNum)
				udpAddr, err := net.ResolveUDPAddr("udp4", Service)
				checkError(err)
				conn, err := net.DialUDP("udp", nil, udpAddr)
				checkError(err)

				packet := pk.EncodeTCPsend(pk.TCPsend{destinations, file, false})
				_, err = conn.Write(pk.EncodePacket("send", packet))
				checkError(err)

				var buf [512]byte
				_, err = conn.Read(buf[0:])
				checkError(err)

				fmt.Println("send command done from the leader")
			}
		}
	} else { // if that was not the leader, check if a failed processor was a leader
		for _, failed := range failList {
			failedService := failed.IPAddress + ":" + strconv.Itoa(node.DestPortNum)
			fmt.Println(failedService, *node.LeaderServicePtr)
			if failedService == *node.LeaderServicePtr {
				fmt.Println(failedService, "was the leader, starting an election")
				node.initiateElection()
				fmt.Println("NewLeader:", *node.LeaderServicePtr)
			}
		}
	}

	// remove timeout-ed members
	for _, removeit := range removeList {
		node.MsList, removeLog = node.MsList.Remove(removeit)
		removeLogTotal += removeLog
	}
	return node, joinLogTotal + failLog + removeLogTotal
}

/*
AliveMembers()
	RETURN: list of alive members
*/
func (node Node) AliveMembers() []ms.Membership {
	var list []ms.Membership

	allMembers := node.MsList.List

	for _, member := range allMembers {
		if member.Failed == false {
			list = append(list, member)
		}
	}

	return list
}

/*
PickReplicas(n int, originalID ms.Id)
	This function is only called by the leader node

	return nil if there are not enough replicas or the node is not the leader

	RETURN n nodes that can store the replica
*/
func (node Node) PickReplicas(n int, Except []ms.Id) []ms.Id {
	aliveList := node.AliveMembers()
	replicas := []ms.Id{}

	if *(node.IsLeaderPtr) == false {
		fmt.Println("This is not the leader node.")
		return nil
	}

	if len(aliveList) < n {
		fmt.Println("Not enough alive nodes. There are", len(aliveList), "alive nodes, but we need", n, "alive nodes.")
		return nil
	}

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	curr := r1.Intn(len(aliveList))
	for count := 0; count < n; count++ {
		member := aliveList[curr]
		curr = (curr + 1) % len(aliveList)

		flag := false

		for _, exception := range Except {
			if member.ID == exception {
				count--
				flag = true
				break
			}
		}

		if flag {
			continue
		}

		replicas = append(replicas, member.ID)
	}

	return replicas
}

/*
	makeRing(AliveMembers []ms.Membership) (int, []string)

	From alive members, it creates a []string(ring) that will be used
	throughout the ring leader election protocol

	returns index of current node within the ring, and the ring itself.
*/
func (node Node) makeRing(AliveMembers []ms.Membership) (int, []string) {
	var ring []string
	var tempService string
	myIndex := 1

	for i, member := range AliveMembers {
		tempService = member.ID.IPAddress + ":" + strconv.Itoa(node.DestPortNum)
		if tempService == node.MyService {
			myIndex = i
		}
		ring = append(ring, tempService)
	}

	return myIndex, ring
}

/*
	initiateElection()

	Initiate an election by setting itself as a leader and an initiator
*/
func (node Node) initiateElection() {
	AliveMembers := node.AliveMembers()

	Myindex, ring := node.makeRing(AliveMembers)

	Initator := ring[Myindex]
	NewLeader := ring[Myindex]
	if *node.ElectionInitiatorPtr != "" && Initator < *node.ElectionInitiatorPtr {
		return
	}
	*node.ElectionInitiatorPtr = Initator

	nextIndex := (Myindex + 1) % len(ring)
	nextService := ring[nextIndex]
	udpAddr, err := net.ResolveUDPAddr("udp4", nextService)
	checkError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	checkError(err)

	packet := pk.RingData{false, nextIndex, ring, Initator, NewLeader}
	_, err = conn.Write(pk.EncodePacket("election", pk.EncodeRingData(packet)))

	var buf [4096]byte
	n, err := conn.Read(buf[0:])
	checkError(err)
	receivedPacket := pk.DecodePacket(buf[0:n])

	fmt.Println("Received Message at the end of initiateElection():", receivedPacket.Ptype)
}

/*
	SendElection(packet pk.RingData)

	Pass the election message to its successor
*/
func SendElection(packet pk.RingData) {

	ring := packet.Ring
	destIndex := packet.YourIndex

	nextService := ring[destIndex]
	udpAddr, err := net.ResolveUDPAddr("udp4", nextService)
	checkError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	checkError(err)

	_, err = conn.Write(pk.EncodePacket("election", pk.EncodeRingData(packet)))

	var buf []byte
	_, err = conn.Read(buf[0:])
	checkError(err)

}

// check for errors
func errorCheck(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}

func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
