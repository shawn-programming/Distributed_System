package servent

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	ms "../Membership"
	nd "../Node"
)

/*
struct for Packet used in UDP communication
*/
type Packet struct {
	Input            ms.MsList //input msList of the sender process
	IsInitialization bool
}

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

		byteSent, err = conn.Write([]byte(msg))
		CheckError(err)

		var buf [512]byte
		n, err := conn.Read(buf[0:])
		CheckError(err)
		receivedMsg := string(buf[0:n])
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
	message := Packet{memberships, IsInitialization}
	encodedMessage := EncodeJSON(message)
	byteSent, err := conn.Write(encodedMessage)

	CheckError(err)

	// if this is not an initial ping, return a empty list
	if !IsInitialization {
		return ms.MsList{}, byteSent
	}

	var n int
	var response [5120]byte
	var decodedResponse Packet
	n, err = conn.Read(response[0:])
	decodedResponse = DecodeJSON(response[:n])
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
ListenOnPort(conn *net.UDPConn, isIntroducer bool, node nd.Node, ATApointer *bool)

	Open server so that other processors can send data to this processor
	1) If []msList data is received, return that list to be used for updating membership list
	2) If a string is received, execute special instruction (changing to gossip or all to all or etc)
	else do nothing

	RETURN: msList, log

*/
func ListenOnPort(conn *net.UDPConn, isIntroducer bool, node nd.Node, ATApointer *bool, destPortNum int, failRate int) (ms.MsList, string) {
	var portLog string
	var buf [5120]byte
	n, addr, err := conn.ReadFromUDP(buf[0:])
	if err != nil {
		fmt.Println("err != nil")
		return ms.MsList{}, ""
	}

	// special command received
	gossip := []byte("gossip")
	ata := []byte("ata")
	if n == len(gossip) {
		fmt.Println("changing to gossip")
		portLog = "changing to gossip"
		*ATApointer = false
		conn.WriteToUDP([]byte(node.Id.IPAddress+" turned into gossip"), addr)
		return ms.MsList{}, portLog
	} else if n == len(ata) {
		fmt.Println("changing to ATA")
		portLog = "changing to ATA"
		*ATApointer = true
		conn.WriteToUDP([]byte(node.Id.IPAddress+" turned into ata"), addr)
		return ms.MsList{}, portLog
	}

	// heartbeat received
	var message Packet
	message = DecodeJSON(buf[:n])

	if isIntroducer && message.IsInitialization { // if this processor is a introducer and there is newly joined processor to the system
		currMsList := node.MsList
		currMsList = currMsList.Add(message.Input.List[0], node.LocalTime)
		encodedMsg := EncodeJSON(Packet{currMsList, false})
		conn.WriteToUDP([]byte(encodedMsg), addr)
		if *ATApointer == true {

			_ = PingMsg(node, currMsList, "ata", destPortNum)
		} else {
			_ = PingMsg(node, currMsList, "gossip", destPortNum)
		}

		return currMsList, portLog
	} else { // message is not an initialization message

		// message is dropped for failrate
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)
		if r1.Intn(100) < failRate {
			return ms.MsList{}, ""
		}
		// temp = random() % (100/fail_rate)
		// temp == 0:
		// 	return ms.MsList{}, ""
		return message.Input, portLog
	}
}

/* ##################################
########## UTILITY FUNCTIONS ###########
*/

/*
EncodeJSON(message Packet) []byte
	Encodes message Packet into byte slice
*/
func EncodeJSON(message Packet) []byte {
	encodedMessage, err := json.Marshal(message)
	CheckError(err)
	return encodedMessage
}

/*
DecodeJSON(encodedMessage []byte) Packet
	Decodes byte slice into message Packet
*/
func DecodeJSON(encodedMessage []byte) Packet {
	var decodedMessage Packet
	err := json.Unmarshal(encodedMessage, &decodedMessage)
	CheckError(err)
	return decodedMessage
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
*/
func GetCommand(ATA *bool, loggerByte, logger, loggerPerSec *log.Logger, processNodePtr *nd.Node, destPortNum int, vmNumStr, myService string) {
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
			fmt.Println("gossip		:	change the system into a gossip heartbeating")
			fmt.Println("ata		:	change the system into a All-to-All heartbeating")
			fmt.Println("leave		: 	voluntarily leave the system. (halt)")
			fmt.Println("memberlist	: 	print VM's memberlist to the terminal")
			fmt.Println("id		:	print current IP address and assigned Port number")
		} else if command == "heartbeat" {
			if *ATA == true {
				fmt.Println("Current Heartbeating for this processor: ATA")
			} else {
				fmt.Println("Current Heartbeating for this processor: Gossip")
			}
		} else {
			fmt.Println("Invalid Command")
		}
	}

}
