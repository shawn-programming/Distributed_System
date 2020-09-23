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

func main() {

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s need a VM number", os.Args[0])
		os.Exit(1)
	}
	scanner := bufio.NewScanner(os.Stdin)
	ATA := true
	go func() {
		for {
			scanner.Scan()
			command := scanner.Text()

			if command == "gossip" {
				fmt.Println("Changing to Gossip")
				ATA = false
			} else if command == "ata" {
				fmt.Println("Changing to ATA")
				ATA = true
			} else {
				fmt.Println("Invalid Command")
			}
		}
	}()

	K, _ := config.K()
	vmNum, err := strconv.Atoi(os.Args[1])
	vmNumStr := os.Args[1]
	IntroducerIPList, _ := config.IPAddress() // get Introducer's IP from config.json
	IntroducerIP := IntroducerIPList[0]
	portList, _ := config.Port() // get port number from config.json
	// portNum := portList[vmNum]
	timeOut, _ := config.TimeOut() // get time out info from config.json
	isIntroducer := vmNum == 0
	selfIP := IntroducerIPList[vmNum]

	myPortNum := portList[0]
	destPortNum := portList[0]

	myService := selfIP + ":" + strconv.Itoa(myPortNum)
	serverID := generateID() // default value for the introducer
	processNode := nd.CreateNode(serverID, selfIP, 0, timeOut)

	fmt.Println("ServerID:", serverID)
	fmt.Println("selfIP:", myService)
	fmt.Println("TimoOut:", timeOut)
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
			InputList = append(InputList, ListenOnPort(conn, isIntroducer, processNode))

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

type Packet struct {
	Input            ms.MsList
	IsInitialization bool
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
	message := Packet{memberships, IsInitialization}
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
func ListenOnPort(conn *net.UDPConn, isIntroducer bool, node nd.Node) ms.MsList {
	fmt.Println("ListenOnPort")
	var buf [5120]byte
	fmt.Println("start reading")
	n, addr, err := conn.ReadFromUDP(buf[0:])
	fmt.Println("done reading")
	if err != nil {
		fmt.Println("err != nil")
		return ms.MsList{}
	}
	fmt.Println("UDPmessage received")
	var message Packet
	fmt.Println("decoding....")
	message = decodeJSON(buf[:n])
	fmt.Println("received message: ")
	message.Input.Print()

	if isIntroducer && message.IsInitialization { // server is introducer and message is an initialization message
		currMsList := node.MsList
		currMsList = currMsList.Add(message.Input.List[0], node.LocalTime)
		fmt.Println("CurrMsList: ")
		currMsList.Print()
		encodedMsg := encodeJSON(Packet{currMsList, false})
		conn.WriteToUDP([]byte(encodedMsg), addr)
		return currMsList
	} else { // server is introducer but message is not an initialization message
		// message.Input.Print()
		// fmt.Println("not ")
		return message.Input
	}
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
