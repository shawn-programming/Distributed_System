package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	_ "log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	"../../log/config"
	ms "../Membership"
	nd "../Node"
)

// func (node nd.Node) sendMsgToMembers(msg []byte, portNum int) {
// 	currList := node.MsList

// 	for _, membership := range currList.List {
// 		if membership.ID.IdNum == node.Id.IdNum {
// 			continue
// 		}
// 		sendMessageToMember(node, membership.ID.IPAddress, msg, portNum)
// 	}
// }

// func sendMsgToMember(node nd.Node, IPAddress ms.ID.IPAddress, msg []byte, portNum int) {
// 	targetServicee := targetIP + ":" + strconv.Itoa(portNum)
// 	udpAddr, err := net.ResolveUDPAddr("udp4", targetServicee)
// 	checkError(err)

// 	conn, err := net.DialUDP("udp", nil, udpAddr)
// 	checkError(err)
// 	sendMessage()
// }

// func sendMessage(conn *net.UDPConn, msg []byte) {
// 	n, err := conn.Write(msg)
// 	fmt.Println("bytessent:", n)
// 	checkError(err)

// 	fmt.Println("reading response...")
// 	var response [5120]byte
// 	n, err = conn.Read(response[0:])
// 	fmt.Println("bytesread:", n)

// }

func pingMsg(node nd.Node, msg string, portNum int) {
	memList := node.MsList

	for _, member := range memList.List {
		if member.ID.IdNum == node.Id.IdNum {
			continue
		}
		service := member.ID.IPAddress + ":" + strconv.Itoa(portNum)

		udpAddr, err := net.ResolveUDPAddr("udp4", service)
		checkError(err)

		conn, err := net.DialUDP("udp", nil, udpAddr)
		checkError(err)

		_, err = conn.Write([]byte([]byte(msg)))
		checkError(err)

		var buf [512]byte
		n, err := conn.Read(buf[0:])
		checkError(err)

		receivedMsg := string(buf[0:n])
		fmt.Println(receivedMsg)
	}
}

func main() {
	K, _ := config.K()
	vmNum, err := strconv.Atoi(os.Args[1])
	vmNumStr := os.Args[1]
	IntroducerIPList, _ := config.IPAddress() // get Introducer's IP from config.json
	IntroducerIP := IntroducerIPList[1]
	portList, _ := config.Port() // get port number from config.json
	// portNum := portList[vmNum]
	timeOut, _ := config.TimeOut() // get time out info from config.json
	isIntroducer := vmNum == 1
	selfIP := IntroducerIPList[vmNum]

	myPortNum := portList[vmNum-1]
	destPortNum := portList[(vmNum)%2]

	myService := selfIP + ":" + strconv.Itoa(myPortNum)
	serverID := generateID() // default value for the introducer
	processNode := nd.CreateNode(serverID, selfIP, 0, timeOut)

	fmt.Println("ServerID:", serverID)
	fmt.Println("selfIP:", myService)
	fmt.Println("TimoOut:", timeOut)

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s need a VM number", os.Args[0])
		os.Exit(1)
	}

	var command string
	scanner := bufio.NewScanner(os.Stdin)
	ATA := true
	go func() {
		for {
			scanner.Scan()
			if command == "gossip" {
				fmt.Println("Changing to Gossip")
				pingMsg(processNode, "gossip", destPortNum)
				ATA = false

			} else if command == "ata" {
				fmt.Println("Changing to ATA")
				pingMsg(processNode, "ata", destPortNum)
				ATA = true
			} else {
				fmt.Println("Invalid Command")
			}
		}
	}()

	fmt.Println(" ================== open server and logging system ==================")

	udpAddr, err := net.ResolveUDPAddr("udp4", myService)
	checkError(err)

	conn, err := net.ListenUDP("udp", udpAddr)
	checkError(err)

	f, err := os.OpenFile("Processor_"+vmNumStr+".log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()

	//logger := log.New(f, "Processor_"+vmNumStr, log.LstdFlags)

	// if newly joined introducer, notify the introducer and update its membership List
	if !isIntroducer {
		fmt.Println("Connecting to Introducer...")
		received := sendMessageToOne(processNode, IntroducerIP, destPortNum, true)
		processNode.MsList = received
		fmt.Println("Connected!")
	}

	// a placeholder for input membership List from other processors
	InputList := []ms.MsList{}
	newList := InputList

	fmt.Println("-------starting listening----------")
	go func(conn *net.UDPConn, isIntroducer bool, processNode nd.Node) {
		for {
			InputList = append(InputList, ListenOnPort(conn, isIntroducer, processNode, &ATA))

		}
	}(conn, isIntroducer, processNode)

	fmt.Println("----------Start Sending----------")
	for {
		newList = InputList
		InputList = []ms.MsList{}
		processNode = processNode.IncrementLocalTime(newList)
		pingToOtherProcessors(destPortNum, processNode, ATA, K)
	}
}

//----------Packet--------------------
type Packet struct {
	dataType         int8
	Input            ms.MsList
	IsInitialization bool
}

// set dataType of Packet value to 1
func (obj *Packet) makePacket() {
	if obj.dataType == 0 {
		obj.dataType = 1
	}
}

//-----------MessagePacket-------------
type MessagePacket struct {
	dataType int8
}

// set dataType of MessagePacket value to 1
func (obj *MessagePacket) makeMessagePacket() {
	if obj.dataType == 0 {
		obj.dataType = 2
	}
}

func pingToOtherProcessors(portNum int, node nd.Node, ATA bool, K int) {
	fmt.Println("-----pingToOtherProcessors-----")

	currList := node.MsList
	fmt.Println("currList length: ", len(currList.List))
	currList.Print()

	if ATA {
		fmt.Println("current status(ata):", ATA)
		for _, membership := range currList.List {
			if membership.ID.IdNum == node.Id.IdNum {
				continue
			}
			sendMessageToOne(node, membership.ID.IPAddress, portNum, false)
		}
	} else {
		fmt.Println("current status(gossip):", !ATA)
		receiverList := selectRandomProcess(K, node)
		for _, receiver := range receiverList {
			membership := currList.List[receiver]
			sendMessageToOne(node, membership.ID.IPAddress, portNum, false)
		}

	}
}

func selectRandomProcess(k int, node nd.Node) []int {
	list := []int{}
	size := len(node.MsList.List)
	msList := node.MsList.List
	for i := 0; i < size; i++ {
		list = append(list, i)
	}

	for i, member := range msList {
		if node.Id.IdNum == member.ID.IdNum {
			list = append(list[:i], list[i+1:]...)
		}
	}

	//s := rand.NewSource(time.Now().UnixNano())
	for {
		if len(list) >= k || len(list) == 0 {
			return list
		}
		randomNumber := rand.Int() % len(list)
		list = append(list[:randomNumber], list[randomNumber+1:]...)
	}
}

func ping(conn *net.UDPConn, memberships ms.MsList, IsInitialization bool) ms.MsList {
	message := Packet{1, memberships, IsInitialization}
	message.makePacket()
	//var encodedMessage []byte
	fmt.Println("-----Ping-----")
	encodedMessage := encodeJSON(message)

	fmt.Println("encoding...")
	n, err := conn.Write(encodedMessage)
	fmt.Println("bytessent:", n)
	checkError(err)

	if !IsInitialization {
		return ms.MsList{}
	}

	fmt.Println("reading response...")
	var response [5120]byte
	var decodedResponse Packet
	decodedResponse.makePacket()
	n, err = conn.Read(response[0:])
	fmt.Println("bytesread:", n)
	fmt.Println("decoding...")
	decodedResponse = decodeJSON(response[:n])
	fmt.Println("decoding done")
	decodedResponse.Input.Print()
	return decodedResponse.Input
}

// send membershipList to one processor
func sendMessageToOne(node nd.Node, targetIP string, portNum int, IsInitialization bool) ms.MsList {
	fmt.Println("------sendMessageToOne-----")
	targetServicee := targetIP + ":" + strconv.Itoa(portNum)
	udpAddr, err := net.ResolveUDPAddr("udp4", targetServicee)
	checkError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	checkError(err)
	received := ping(conn, node.MsList, IsInitialization)

	return received
}

// Listen to incoming messages (membershipList)
func ListenOnPort(conn *net.UDPConn, isIntroducer bool, node nd.Node, ATApointer *bool) ms.MsList {
	fmt.Println("ListenOnPort")
	var buf [5120]byte
	fmt.Println("start reading")
	n, _, err := conn.ReadFromUDP(buf[0:])
	fmt.Println("done reading")
	if err != nil {
		fmt.Println("err != nil")
		return ms.MsList{}
	}
	gossip := []byte("gossip")
	ata := []byte("ata")
	if n == len(gossip) {
		fmt.Println("changing to gossip")
		*ATApointer = false
		return ms.MsList{}
	} else if n == len(ata) {
		fmt.Println("changing to ATA")
		*ATApointer = true
		return ms.MsList{}
	}

	fmt.Println("UDPmessage received")

	fmt.Println("---------------testing msgPING---------------")
	fmt.Println(buf[:n])

	var message Packet
	message.makePacket()
	var metadata MessagePacket
	metadata.makeMessagePacket()

	/*switch dtype:=buf[0];dtype{
	case 1:
		fmt.Println("memberList received, start decoding")
		message = decodeJSON(buf[1:n])
		message.Print()
	case 2:
		fmt.Println("metadata received")
		metadata = decodeJson(buf[1:n])
		//metadata.Print()
	}*/

	/*fmt.Println("decoding....")
	message = decodeJSON(buf[1:n])
	fmt.Println("received message: ")
	message.Input.Print()

	if isIntroducer && message.IsInitialization { // server is introducer and message is an initialization message
		currMsList := node.MsList
		currMsList = currMsList.Add(message.Input.List[0], node.LocalTime)
		fmt.Println("CurrMsList: ")
		currMsList.Print()
		encodedMsg := encodeJSON(Packet{1, currMsList, false})
		conn.WriteToUDP([]byte(encodedMsg), addr)
		return currMsList
	} else { // server is introducer but message is not an initialization message
		// message.Input.Print()
		// fmt.Println("not ")
		return message.Input
	}*/

	return ms.MsList{}
}

// // Listen to incoming messages (membershipList)
// func temp(conn *net.UDPConn, isIntroducer bool, node nd.Node) ms.MsList {

// 	var buf [1024]byte
// 	n, _, err := conn.ReadFromUDP(buf)
// 	if err != nil || n == 0 {
// 		return ms.MsList{}
// 	}
// 	fmt.Println("UDPmessage received")
// 	var message Packet
// 	fmt.Println("decoding....")
// 	message = decodeJSON(buf[:n])
// 	fmt.Println("received message: ")
// 	message.Input.Print()

// 	if isIntroducer && message.IsInitialization { // server is introducer and message is an initialization message
// 		currMsList := node.MsList
// 		currMsList.Add(message.Input.List[0], node.LocalTime)
// 		currMsList.Print()
// 		return currMsList
// 	} else { // server is introducer but message is not an initialization message
// 		message.Input.Print()
// 		return message.Input
// 	}
// }

// ######################################
// ### encode/decodeJSON ####
// ##########################

//--------------------------------------------------
func encodeJSONMSG(message MessagePacket) []byte {
	encodedMessage, err := json.Marshal(message)
	checkError(err)
	return encodedMessage
}

func decodeJSONMSG(encodedMessage []byte) MessagePacket {
	var decodedMessage MessagePacket
	decodedMessage.makeMessagePacket()
	err := json.Unmarshal(encodedMessage, &decodedMessage)
	checkError(err)
	return decodedMessage
}

//----------------------------------------------------
func encodeJSON(message Packet) []byte {
	encodedMessage, err := json.Marshal(message)
	checkError(err)
	return encodedMessage
}

func decodeJSON(encodedMessage []byte) Packet {
	var decodedMessage Packet
	decodedMessage.makePacket()
	err := json.Unmarshal(encodedMessage, &decodedMessage)
	checkError(err)
	return decodedMessage
}

// #######################################
// ### utility fucntions ####
// ##########################

func generateID() string {
	t := time.Now()
	return t.String()
}

func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
