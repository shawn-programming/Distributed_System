package servent

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	fs "../FileSys"
	ms "../Membership"
	nd "../Node"
	pk "../Packet"
)

/*
	PingMsg(node nd.Node, msg string, portNum int)

	Sends pings containing command data to all members present within current memebrship List
*/
func PingMsg(node nd.Node, memList ms.MsList, msg string, portNum int) int {
	totalBytesSent := 0
	var byteSent int
	// To all the other members, send msg
	for _, member := range memList.List {
		if member.ID.IdNum == node.Id.IdNum {
			continue
		}
		service := member.ID.IPAddress + ":" + strconv.Itoa(portNum)

		udpAddr, err := net.ResolveUDPAddr("udp4", service)
		CheckError(err)

		conn, err := net.DialUDP("udp", nil, udpAddr)
		CheckError(err)

		byteSent, err = conn.Write(pk.EncodePacket(msg, nil))
		CheckError(err)

		var buf [512]byte
		n, err := conn.Read(buf[0:])
		CheckError(err)
		receivedMsg := pk.DecodePacket(buf[0:n]).Ptype
		fmt.Println(receivedMsg)

		totalBytesSent += byteSent
	}

	return totalBytesSent
}

/*
	PingToOtherProcessors(portNum int, node nd.Node, ATA bool, K int)
	Return: log data

	Switch between and execute Gossip and All To All system
*/
func PingToOtherProcessors(portNum int, node nd.Node, ATA bool, K int) (string, int) {
	logMSG := "\n-----pingToOtherProcessors-----\n"

	currList := node.MsList

	logMSG += "currList length: " + strconv.Itoa(len(currList.List)) + "\n"
	logMSG += currList.PrintLog()

	totalBytesSent := 0
	if ATA { // All-To-All style heartbeating
		logMSG += "current status : ata\n"
		for _, membership := range currList.List {
			if membership.ID.IdNum == node.Id.IdNum {
				continue
			}
			_, byteSent := SendMessageToOne(node, membership.ID.IPAddress, portNum, false)
			totalBytesSent += byteSent
		}
	} else { // Gossip style heartbeating
		logMSG += "current status : gossip\n"
		receiverList := SelectRandomProcess(K, node)
		for _, receiver := range receiverList {
			membership := currList.List[receiver]
			_, byteSent := SendMessageToOne(node, membership.ID.IPAddress, portNum, false)
			totalBytesSent += byteSent

		}

	}

	return logMSG, totalBytesSent
}

/*
	SelectRandomProcess(k int, node nd.Node)
	For gossip style, choose at most k members from the membership list except itself.

	RETURN: indices of selected members
*/
func SelectRandomProcess(k int, node nd.Node) []int {
	list := []int{}
	size := len(node.MsList.List)
	msList := node.MsList.List
	for i := 0; i < size; i++ {
		list = append(list, i)
	}
	// remove itself
	for i, member := range msList {
		if node.Id.IdNum == member.ID.IdNum {
			list = append(list[:i], list[i+1:]...)
		}
	}

	// randomly remove until there are <= k members left
	for {
		if len(list) <= k || len(list) == 0 {
			return list
		}
		randomNumber := rand.Int() % len(list)
		list = append(list[:randomNumber], list[randomNumber+1:]...)
	}
}

/*
	Ping(conn *net.UDPConn, memberships ms.MsList, IsInitialization bool) ms.MsList
	Return: response from Ping

	Pings an input member with encoded data Packet and returns response.
*/
func Ping(conn *net.UDPConn, memberships ms.MsList, IsInitialization bool) (ms.MsList, int) {
	message := pk.HBpacket{memberships, IsInitialization}
	encodedMessage := pk.EncodePacket("heartbeat", pk.EncodeHB(message))
	byteSent, err := conn.Write(encodedMessage)

	CheckError(err)

	// if this is not an initial ping, return a empty list
	if !IsInitialization {
		return ms.MsList{}, byteSent
	}

	var n int
	var response [5120]byte
	var decodedResponse pk.HBpacket
	n, err = conn.Read(response[0:])
	decodedResponse = pk.DecodeHB(pk.DecodePacket(response[:n]))
	return decodedResponse.Input, byteSent
}

/*
	SendMessageToOne(node nd.Node, targetIP string, portNum int, IsInitialization bool) ms.MsList
	Return: response from Messaged 	SembershipList to one paessor
*/
func SendMessageToOne(node nd.Node, targetIP string, portNum int, IsInitialization bool) (ms.MsList, int) {
	targetServicee := targetIP + ":" + strconv.Itoa(portNum)
	udpAddr, err := net.ResolveUDPAddr("udp4", targetServicee)
	CheckError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	CheckError(err)
	received, byteSent := Ping(conn, node.MsList, IsInitialization)
	return received, byteSent
}

/*
	OpenServer(conn *net.UDPConn, processNodePtr *nd.Node)
	open the server and collect msgs from other processors
*/
func OpenServer(conn *net.UDPConn, processNodePtr *nd.Node) {
	for {
		_, portLog := ListenOnPort(conn, processNodePtr)

		if len(portLog) > 0 {
			(*processNodePtr).Logger.Println(portLog)
		}
	}
}

/*
	OpenHeartbeat(conn *net.UDPConn, NodePtr *nd.Node)
	open the server for heartbeat and start listening to it
*/

func OpenHeartbeat(conn *net.UDPConn, NodePtr *nd.Node) {
	for {
		tempList, portLog := listenHeartbeat(conn, NodePtr)

		// update InputList to be used for IncrementLocalTime()
		(*(*NodePtr).InputListPtr) = append((*(*NodePtr).InputListPtr), tempList)
		if len(portLog) > 0 {
			(*NodePtr).Logger.Println(portLog)
		}
	}
}

/*
ListenOnPort(conn *net.UDPConn, isIntroducer bool, node nd.Node, ATApointer *bool)

	Open server so that other processors can send data to this processor
	1) If []msList data is received, return that list to be used for updating membership list
	2) If a string is received, execute special instruction (changing to gossip or all to all or etc)
	else do nothing

	RETURN: msList, log

*/
func ListenOnPort(conn *net.UDPConn, nodePtr *nd.Node) (ms.MsList, string) {
	var portLog string
	var buf [5120]byte
	n, addr, err := conn.ReadFromUDP(buf[0:])
	if err != nil {
		fmt.Println("err != nil")
		return ms.MsList{}, ""
	}

	message := pk.DecodePacket(buf[:n])
	messageType := message.Ptype

	// special command received
	if messageType == "gossip" {
		fmt.Println("changing to gossip")
		portLog = "changing to gossip"
		(*(*nodePtr).ATAPtr) = false
		conn.WriteToUDP(pk.EncodePacket((*nodePtr).Id.IPAddress+" turned into gossip", nil), addr)
		return ms.MsList{}, portLog
	} else if messageType == "ata" {
		fmt.Println("changing to ATA")
		portLog = "changing to ATA"
		(*(*nodePtr).ATAPtr) = true
		conn.WriteToUDP(pk.EncodePacket((*nodePtr).Id.IPAddress+" turned into ata", nil), addr)
		return ms.MsList{}, portLog
	} else if messageType == "ReplicaList" { // a processor has sent a request about the list of destinations to store its replica (only a leader should receive this)
		msg := pk.DecodeIdList(message)

		N := nodePtr.MaxFail
		filename := msg.Filename
		originalID := msg.OriginalID
		// replicas stores the List of IDs to send the replicas
		replicas := (*nodePtr).PickReplicas(N, []ms.Id{originalID})

		replicaPackage := pk.IdListpacket{0, ms.Id{"", ""}, replicas, filename}
		replicaEncoded := pk.EncodeIdList(replicaPackage)
		encodedMsg := pk.EncodePacket("ReplicaList", replicaEncoded)
		conn.WriteToUDP(encodedMsg, addr)
		Log := "Sending Replicas"

		return ms.MsList{}, Log
	} else if messageType == "FileNodeList" { // a processor has send a request for the list of nodes that has the file
		msg := pk.DecodeIdList(message)

		filename := msg.Filename

		//send nodes that contains the requested file
		fileNodes := nodePtr.LeaderPtr.FileList[filename]

		fileNodePackage := pk.IdListpacket{0, ms.Id{"", ""}, fileNodes, filename}
		fileNodeEncoded := pk.EncodeIdList(fileNodePackage)

		encodedMsg := pk.EncodePacket("FileNodeList", fileNodeEncoded)
		conn.WriteToUDP(encodedMsg, addr)
		Log := "Sending FileNodes List"

		return ms.MsList{}, Log
	} else if messageType == "updateFileList" { // a process has sent PutListpacket for the leader to update
		msg := pk.DecodePut(message)
		idInfo := msg.Id
		filename := msg.Filename
		encodedMsg := pk.EncodePacket("empty", nil)
		conn.WriteToUDP(encodedMsg, addr)

		// update FileList
		nodePtr.LeaderPtr.FileList[filename] = append(nodePtr.LeaderPtr.FileList[filename], idInfo)

		// update IdList
		nodePtr.LeaderPtr.IdList[idInfo] = append(nodePtr.LeaderPtr.IdList[idInfo], filename)

		return ms.MsList{}, ""
	} else if messageType == "get filelist" { // ls sdfsfilename (leader send the list of all distributed files)
		// make a packet consists of list of files
		FileListEncoded := pk.EncodeFileList(pk.FileListpacket{nodePtr.LeaderPtr.FileList})
		encodedMsg := pk.EncodePacket("file list packet", FileListEncoded)
		conn.WriteToUDP(encodedMsg, addr)
		Log := "sending sdfsfilename List"

		return ms.MsList{}, Log

	} else if messageType == "openTCP" { // a processor has requested to open a TCP port for it
		msg := pk.DecodeTCPcmd(message)
		cmd := msg.Cmd
		fileName := msg.Filename
		isPull := msg.IsPull
		Log := "TCP Opened"

		// open the TCP port
		fs.ListenTCP(cmd, fileName, nodePtr, conn, addr, isPull)

		return ms.MsList{}, Log

	} else if messageType == "send" { // leader has commanded to send the file to processors
		msg := pk.DecodeTCPsend(message)
		IsPull := msg.IsPull
		fileName := msg.Filename
		toList := msg.ToList // processors to send the file

		encodedMsg := pk.EncodePacket("send command received", nil)
		conn.WriteToUDP(encodedMsg, addr)
		Log := "sending files to anothe processor"

		// send the file
		fs.Send(nodePtr, fileName, toList, IsPull)

		return ms.MsList{}, Log

	} else if messageType == "election" { // election ping for electing a leader

		electionPacket := pk.DecodeRingData(message)
		myIndex := electionPacket.YourIndex // my index in the ring
		ring := electionPacket.Ring
		electionPacket.YourIndex = (myIndex + 1) % len(ring) // update the receiver
		newLeader := electionPacket.NewLeader
		initiator := electionPacket.Initiator

		encodedMsg := pk.EncodePacket("election msg received", nil)
		conn.WriteToUDP(encodedMsg, addr)

		if electionPacket.Elected {
			// election is done.
			if newLeader == nodePtr.MyService {
				failedLeader := *nodePtr.LeaderServicePtr
				// electiton intiator ptr is on dormant
				*nodePtr.ElectionInitiatorPtr = ""
				//update current leader to new leader
				*nodePtr.LeaderServicePtr = newLeader
				fmt.Println("Elected Leader: ", newLeader)
				fs.LeaderInit(nodePtr, failedLeader)
			} else {
				//update current leader to new leader
				*nodePtr.LeaderServicePtr = newLeader
				//send the result to the next processor in the ring
				nd.SendElection(electionPacket)
			}
			// a leader hasn't selected yet
		} else {
			if initiator < *nodePtr.ElectionInitiatorPtr {
				// do nothing
			} else {
				*(nodePtr.ElectionInitiatorPtr) = initiator
				if newLeader == nodePtr.MyService { // Leader is the current processor, now let others know the new leader
					electionPacket.Elected = true
				} else if newLeader < nodePtr.MyService {
					//update the packet by making myself as the leader
					electionPacket.NewLeader = nodePtr.MyService
				}
				// pass the result to the next processor in the ring
				nd.SendElection(electionPacket)
			}
		}
		return ms.MsList{}, ""
	} else if messageType == "send a filelist" { // a leader has requested to send the list of files (used for leader initiation)

		Id := nodePtr.Id
		filenames := *(*nodePtr).DistributedFilesPtr //list of files in a distributed folder

		packet := pk.EncodeFilesPacket(pk.FilesPacket{Id, filenames})
		conn.WriteToUDP(pk.EncodePacket("List of files", packet), addr)

		CheckError(err)

		return ms.MsList{}, ""
	} else if messageType == "request" { // a processor has requested a file (pull)
		msg := pk.DecodeTCPsend(message)
		var message string
		destinations := msg.ToList
		filename := msg.Filename

		// file information inside the leader
		fileOwners, exists := nodePtr.LeaderPtr.FileList[filename]

		// check if file exists
		if !exists {
			message = filename + " is not found in the system"
		} else {
			message = ("telling DFs to send a file to you...")
		}

		// reply to the requestor
		encodedMsg := pk.EncodePacket(message, nil)
		conn.WriteToUDP(encodedMsg, addr)

		if exists {
			from := fileOwners[0]
			Service := from.IPAddress + ":" + strconv.Itoa(nodePtr.DestPortNum)
			if Service == nodePtr.MyService { // if the sender is the current node (Leader)
				fs.Send(nodePtr, filename, destinations, true)
			} else { // else tell the service to send the file to the requestor
				udpAddr, err := net.ResolveUDPAddr("udp4", Service)
				CheckError(err)
				conn, err := net.DialUDP("udp", nil, udpAddr)
				CheckError(err)
				packet := pk.EncodeTCPsend(pk.TCPsend{destinations, filename, true})
				// command a fileowner to send the file to the requested process
				_, err = conn.Write(pk.EncodePacket("send", packet))
				CheckError(err)
				var buf [512]byte
				_, err = conn.Read(buf[0:])
				CheckError(err)
			}
		}
		return ms.MsList{}, ""
	} else if messageType == "Remove" { // a processor has requested to remove a file
		filename := string(message.EncodePacket)
		fileOwners, exists := nodePtr.LeaderPtr.FileList[filename]

		//udate filelist
		delete(nodePtr.LeaderPtr.FileList, filename)

		//update idlist
		for id, filelist := range nodePtr.LeaderPtr.IdList {
			file_deleted := 0
			for i, file := range filelist {
				if file == filename {
					to_delete := i - file_deleted
					(*nodePtr.LeaderPtr).IdList[id] = append((*nodePtr.LeaderPtr).IdList[id][:to_delete], (*nodePtr.LeaderPtr).IdList[id][to_delete+1:]...)
					file_deleted += 1
				}
			}
		}

		if exists {
			encodedMsg := pk.EncodePacket("File Found and removed it", nil)
			conn.WriteToUDP(encodedMsg, addr)
		} else {
			encodedMsg := pk.EncodePacket("File not Found", nil)
			conn.WriteToUDP(encodedMsg, addr)
		}

		// make each owner to remove the file
		for _, fileOwner := range fileOwners {
			Service := fileOwner.IPAddress + ":" + strconv.Itoa(nodePtr.DestPortNum)
			if Service == nodePtr.MyService {
				fs.Remove(nodePtr, filename)

			} else {
				udpAddr, err := net.ResolveUDPAddr("udp4", Service)
				CheckError(err)
				conn, err := net.DialUDP("udp", nil, udpAddr)
				CheckError(err)

				_, err = conn.Write(pk.EncodePacket("RemoveFile", []byte(filename)))
				CheckError(err)
				var buf [512]byte
				_, err = conn.Read(buf[0:])
				CheckError(err)
			}
		}
		return ms.MsList{}, ""
	} else if messageType == "RemoveFile" { // remove the file
		filename := string(message.EncodePacket)

		encodedMsg := pk.EncodePacket("Remove request received", nil)
		conn.WriteToUDP(encodedMsg, addr)

		fs.Remove(nodePtr, filename)
		return ms.MsList{}, ""
	}

	fmt.Println("not a valid packet, packet name:", messageType)
	return ms.MsList{}, "string"
}

/*
NewMemberInitialization

Initialize a newly joined member by piniing the introducer
*/
func NewMemberInitialization(nodePtr *nd.Node) {
	IntroducerIP := (*nodePtr).IntroducerIP
	destPortNum := (*nodePtr).DestPortNumHB

	fmt.Println("Connecting to Introducer...")
	(*nodePtr).LoggerPerSec.Println("Connecting to Introducer...")
	(*nodePtr).Logger.Println("Connecting to Introducer...")

	received, byteSent := SendMessageToOne((*nodePtr), IntroducerIP, destPortNum, true)

	(*(*nodePtr).TotalByteSentPtr) += byteSent

	(*nodePtr).LoggerByte.Println(string(time.Now().Format(time.RFC1123)))
	(*nodePtr).LoggerByte.Println("Byte sent 		: " + strconv.Itoa(byteSent) + " Bytes.")
	(*nodePtr).LoggerByte.Println("Total byte sent	: " + strconv.Itoa((*(*nodePtr).TotalByteSentPtr)) + " Bytes.\n")

	(*nodePtr).MsList = received
	fmt.Println("Connected!")
	(*nodePtr).LoggerPerSec.Println("Connected!")
	(*nodePtr).Logger.Println("Connected!")
}

/*
	listenHeartbeat(conn *net.UDPConn, nodePtr *nd.Node) (ms.MsList, string)

	listen to incoming heartbeat and update its memberlist accordingly.
*/
func listenHeartbeat(conn *net.UDPConn, nodePtr *nd.Node) (ms.MsList, string) {
	var portLog string
	var buf [5120]byte

	n, addr, err := conn.ReadFromUDP(buf[0:])
	//fmt.Print("n:", n)
	if err != nil {
		fmt.Println("err != nil")
		return ms.MsList{}, ""
	}
	//fmt.Println("read done")

	message := pk.DecodePacket(buf[:n])
	messageType := message.Ptype

	if messageType == "heartbeat" {
		// heartbeat received
		// fmt.Println("heartbeat")
		msg := pk.DecodeHB(message)

		if (*nodePtr).IsIntroducer && msg.IsInitialization { // if this processor is a introducer and there is newly joined processor to the system
			currMsList := (*nodePtr).MsList
			currMsList = currMsList.Add(msg.Input.List[0], (*nodePtr).LocalTime)
			encodedMsg := pk.EncodePacket("heartbeat", pk.EncodeHB(pk.HBpacket{currMsList, false}))
			conn.WriteToUDP(encodedMsg, addr)
			if (*(*nodePtr).ATAPtr) == true {
				_ = PingMsg((*nodePtr), currMsList, "ata", (*nodePtr).DestPortNum)
			} else {
				_ = PingMsg((*nodePtr), currMsList, "gossip", (*nodePtr).DestPortNum)
			}

			return currMsList, portLog
		} else { // message is not an initialization message
			// message is dropped for failrate
			s1 := rand.NewSource(time.Now().UnixNano())
			r1 := rand.New(s1)
			if r1.Intn(100) < (*nodePtr).FailRate {
				return ms.MsList{}, ""
			}

			return msg.Input, portLog
		}
	} else {
		fmt.Println("Invalid HeartBeat:", messageType)
		return ms.MsList{}, portLog
	}
}

/*
	Heartbeat(nodePtr *nd.Node)

	Send heartbeat to other processors to signal its aliveness
*/
func Heartbeat(nodePtr *nd.Node) {
	loggerPerSec := (*nodePtr).LoggerPerSec
	logger := (*nodePtr).Logger
	loggerByte := (*nodePtr).LoggerByte

	for {
		newList := (*(*nodePtr).InputListPtr)

		(*(*nodePtr).InputListPtr) = []ms.MsList{}
		var logStr string
		// update the processor's membership list
		(*nodePtr), logStr = (*nodePtr).IncrementLocalTime(newList)

		if logStr != "" {
			loggerPerSec.Println(logStr)
			logger.Println(logStr)
		}
		// sned the processor's member to other processors
		logPerSec, byteSent := PingToOtherProcessors((*nodePtr).DestPortNumHB, (*nodePtr), (*(*nodePtr).ATAPtr), (*nodePtr).K)
		loggerPerSec.Println(logPerSec)

		(*(*nodePtr).TotalByteSentPtr) += byteSent

		if byteSent != 0 {
			loggerByte.Println(string(time.Now().Format(time.RFC1123)))
			loggerByte.Println("Byte sent 		: " + strconv.Itoa(byteSent) + " Bytes.")
			loggerByte.Println("Total byte sent	: " + strconv.Itoa((*(*nodePtr).TotalByteSentPtr)) + " Bytes.\n")
		}
	}
}

/*
CheckError(err error)
	Terminate system with message, if Error occurs
*/
func CheckError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}

/*
GetCommand(ATA *bool, logger, loggerPerSec *log.Logger, processNode nd.Node, destPortNum int, vmNumStr, myService string)
	Executes following commands

			gossip:		change the system into a gossip heartbeating
			ata:		change the system into a All-to-All heartbeating
			leave: 		voluntarily leave the system. (halt)
			memberlist: print VM's memberlist to the terminal
			id:			print current IP address and assigned Port number
			-h: 	 	print list of commands

			put <filename>:		put file into DFS
			pull <filename>: 	get a file from DFS
			remove <filename>:	remove a file from DFS

			ls:			show all file and where it is stored in DFS
			ls <filename> show where filename is currently stored in DFS
			store: 		show all file stored in current node
			ls -l:		show file names and its size of the files stored in the distributed folder
*/
func GetCommand(processNodePtr *nd.Node) {
	ATA := &(*(*processNodePtr).ATAPtr)
	logger := (*processNodePtr).Logger
	loggerByte := (*processNodePtr).LoggerByte
	loggerPerSec := (*processNodePtr).LoggerPerSec
	destPortNum := (*processNodePtr).DestPortNum
	vmNumStr := (*processNodePtr).VmNumStr
	myService := (*processNodePtr).MyService

	scanner := bufio.NewScanner(os.Stdin)
	byteSent := 0
	for {
		scanner.Scan()
		command := scanner.Text()

		if command == "gossip" {
			fmt.Println("Changing to Gossip")
			loggerPerSec.Println("Changing to Gossip")
			logger.Println("Changing to Gossip")
			*ATA = false
			byteSent = PingMsg(*processNodePtr, (*processNodePtr).MsList, "gossip", destPortNum)
			loggerByte.Println("Command(Gossip) Ping ByteSent:" + strconv.Itoa(byteSent) + "bytes")

		} else if command == "ata" {
			fmt.Println("Changing to ATA")
			*ATA = true
			byteSent = PingMsg(*processNodePtr, (*processNodePtr).MsList, "ata", destPortNum)
			loggerPerSec.Println("Changing to ATA")
			logger.Println("Changing to ATA")

			loggerByte.Println("Command(ATA) Ping ByteSent:" + strconv.Itoa(byteSent) + "bytes")

		} else if command == "leave" {
			fmt.Println("(Leave)Terminating vm_", vmNumStr)
			loggerPerSec.Println("(Leave)Terminating vm_" + vmNumStr)
			logger.Println("(Leave)Terminating vm_" + vmNumStr)
			os.Exit(1)
		} else if command == "memberlist" {
			fmt.Println("\nMembership List: \n" + (*processNodePtr).MsList.PrintLog())
			loggerPerSec.Println("\nMembership List: \n" + (*processNodePtr).MsList.PrintLog())
			logger.Println("\nMembership List: \n" + (*processNodePtr).PrintLog())
		} else if command == "id" {
			fmt.Println("Current IP and port:", myService)
			loggerPerSec.Println("\nCurrent IP and port: " + myService + "\n")
			logger.Println("\nCurrent IP and port:: " + myService + "\n")
		} else if command == "-h" {
			fmt.Println("gossip				:	change the system into a gossip heartbeating")
			fmt.Println("ata				:	change the system into a All-to-All heartbeating")
			fmt.Println("leave				: 	voluntarily leave the system. (halt)")
			fmt.Println("memberlist			: 	print VM's memberlist to the terminal")
			fmt.Println("id					:	print current IP address and assigned Port number")
			fmt.Println("heartbeat			:	print the current heartbeat type")
			fmt.Println("put <filename>		:   put a <filename> to the distributed system")
			fmt.Println("pull <filename>	:   pull a <filename> from the distributed system and store in the the local folder")
			fmt.Println("ls -l				:	print the list of distributed files and its size in the current process")
			fmt.Println("ls 				:	print the list of sdfsfile's in the system")
			fmt.Println("ls <filename>		:	print the list of IDs having a file <filename>")
			fmt.Println("store				:	print the list of distributed's in the process")
			fmt.Println("remove <filename>	:	remove the <filename> from the system")
		} else if command == "heartbeat" {
			if *ATA == true {
				fmt.Println("Current Heartbeating for this processor: ATA")
			} else {
				fmt.Println("Current Heartbeating for this processor: Gossip")
			}
		} else if len(command) > 3 && command[:3] == "put" {
			filename := command[4:]
			fs.Put(processNodePtr, filename, 1)

		} else if len(command) > 4 && command[:4] == "pull" {
			filename := command[5:]
			fs.Pull(processNodePtr, filename, 1)

		} else if command == "ls -l" { // list file names and its size of the files stored in the distributed folder
			files, err := ioutil.ReadDir(processNodePtr.DistributedPath)
			CheckError(err)

			for i, file := range files {
				fmt.Println(strconv.Itoa(i)+". "+file.Name()+":", file.Size(), "bytes")
			}
		} else if command[0:2] == "ls" {
			Filenames := fs.GetFileList(processNodePtr)

			if len(command) > 2 { // list all machine (VM) addresses where this file is currently being stored
				filename := command[3:]

				_, exist := Filenames[filename]

				if !exist {
					fmt.Println("no such file exist in DFS")
				} else {
					for file, IPAddressList := range Filenames {
						if filename == file {
							fmt.Println("File ", file, "is stored in the following Addresses:")
							for i, ID := range IPAddressList {
								fmt.Println("	", i, ":", ID.IPAddress)
							}
						}
					}
				}
			} else { // list all file info
				for file, IPAddressList := range Filenames {
					fmt.Println("File ", file, "is stored in the following Addresses:")
					for i, ID := range IPAddressList {
						fmt.Println("	", i, ":", ID.IPAddress)
					}
				}
			}

		} else if command == "store" { //list all files currently being stored at this machine
			fmt.Println("Files currently stored at this machine:")
			for _, file := range *processNodePtr.DistributedFilesPtr {
				fmt.Println(file)
			}

		} else if len(command) > 6 && command[:6] == "remove" { // remove the file
			filename := command[7:]
			fs.RemoveFile(processNodePtr, filename)

		} else {
			fmt.Println("Invalid Command")
		}
	}
}
