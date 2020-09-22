package main

import (
	"encoding/json"
	"fmt"
	_ "log"
	"net"
	"os"
	"strconv"
	"time"

	"../../log/config"
	ms "../Membership"
	nd "../Node"
)

func main() {

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s need a VM number", os.Args[0])
		os.Exit(1)
	}
	vmNum, err := strconv.Atoi(os.Args[1])
	vmNumStr := os.Args[1]
	IntroducerIPList, _ := config.IPAddress() // get Introducer's IP from config.json
	IntroducerIP := IntroducerIPList[0]
	portList, _ := config.Port() // get port number from config.json
	portNum := portList[0]
	timeOut, _ := config.TimeOut() // get time out info from config.json
	isIntroducer := vmNum == 0
	selfIP := IntroducerIPList[vmNum]
	myService := selfIP + ":" + strconv.Itoa(portNum)
	serverID := generateID() // default value for the introducer
	processNode := nd.CreateNode(serverID, selfIP, 0, timeOut)

	fmt.Println("Current Server:", myService)

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
		received := sendMessageToOne(processNode, IntroducerIP, portNum, true)
		processNode.MsList = received
		fmt.Println("Connected!")
	}

	// a placeholder for input membership List from other processors
	InputList := []ms.MsList{}

	fmt.Println("-------starting listening----------")
	// Listen to incoming messages
	go func(conn *net.UDPConn, isIntroducer bool, processNode nd.Node) {
		for {
			InputList = append(InputList, ListenOnPort(conn, isIntroducer, processNode))
		}
	}(conn, isIntroducer, processNode)
	fmt.Println("----------Start Sending----------")
	for {
		newList := InputList
		InputList = []ms.MsList{}
		processNode = processNode.IncrementLocalTime(newList)
		pingToOtherProcessors(portNum, processNode)
	}

}

type Packet struct {
	Input            ms.MsList
	IsInitialization bool
}

func pingToOtherProcessors(portNum int, node nd.Node) {
	fmt.Println("-----pingToOtherProcessors-----")

	currList := node.MsList

	for _, membership := range currList.List {
		if membership.ID.IdNum == node.Id.IdNum {
			continue
		}
		sendMessageToOne(node, membership.ID.IPAddress, portNum, false)
	}
}

func ping(conn *net.UDPConn, memberships ms.MsList, IsInitialization bool) ms.MsList {
	message := Packet{memberships, IsInitialization}
	//var encodedMessage []byte
	fmt.Println("-----Ping-----")
	encodedMessage := encodeJSON(message)

	n, err := conn.Write(encodedMessage)
	checkError(err)

	if !IsInitialization {
		return ms.MsList{}
	}

	var response []byte
	var decodedResponse Packet
	n, err = conn.Read(response)
	decodedResponse = decodeJSON(response[:n])
	return decodedResponse.Input
}

// send membershipList to one processor
func sendMessageToOne(node nd.Node, targetIP string, portNum int, IsInitialization bool) ms.MsList {
	fmt.Println("------sendMessageToOne-----")
	targetServicee := targetIP + ":" + string(portNum)
	udpAddr, err := net.ResolveUDPAddr("udp4", targetServicee)
	checkError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	checkError(err)
	received := ping(conn, node.MsList, IsInitialization)

	return received
}

// Listen to incoming messages (membershipList)
func ListenOnPort(conn *net.UDPConn, isIntroducer bool, node nd.Node) ms.MsList {

	var buf []byte
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil || n == 0 {
		return ms.MsList{}
	}

	var message Packet
	message = decodeJSON(buf[:n])

	if isIntroducer && message.IsInitialization { // server is introducer and message is an initialization message
		currMsList := node.MsList
		currMsList.Add(message.Input.List[0], node.LocalTime)
		return currMsList
	} else { // server is introducer but message is not an initialization message
		return message.Input
	}
}

// ######################################
// ### encode/decodeJSON ####
// ##########################

func encodeJSON(message Packet) []byte {
	encodedMessage, err := json.Marshal(message)
	checkError(err)
	return encodedMessage
}

func decodeJSON(encodedMessage []byte) Packet {
	var decodedMessage Packet
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
