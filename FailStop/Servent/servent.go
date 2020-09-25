package servent

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"

	ms "../Membership"
	nd "../Node"
)

type Packet struct {
	Input            ms.MsList //input msList of the sender process
	IsInitialization bool
}

func PingMsg(node nd.Node, msg string, portNum int) {
	memList := node.MsList

	for _, member := range memList.List {
		if member.ID.IdNum == node.Id.IdNum {
			continue
		}
		service := member.ID.IPAddress + ":" + strconv.Itoa(portNum)
		// fmt.Println(member.ID.IPAddress)

		udpAddr, err := net.ResolveUDPAddr("udp4", service)
		CheckError(err)

		conn, err := net.DialUDP("udp", nil, udpAddr)
		CheckError(err)

		_, err = conn.Write([]byte([]byte(msg)))
		CheckError(err)

		var buf [512]byte
		n, err := conn.Read(buf[0:])
		CheckError(err)
		receivedMsg := string(buf[0:n])
		fmt.Println(receivedMsg)
	}
}

func PingToOtherProcessors(portNum int, node nd.Node, ATA bool, K int) string {
	log := "\n-----pingToOtherProcessors-----\n"
	// fmt.Println("-----pingToOtherProcessors-----")

	currList := node.MsList
	// fmt.Println("currList length: ", len(currList.List))
	log += "currList length: " + strconv.Itoa(len(currList.List)) + "\n"
	// currList.Print()
	log += currList.PrintLog()

	if ATA {
		log += "current status : ata\n"
		// fmt.Println("current status : ata")
		for _, membership := range currList.List {
			if membership.ID.IdNum == node.Id.IdNum {
				continue
			}
			SendMessageToOne(node, membership.ID.IPAddress, portNum, false)
		}
	} else {
		log += "current status : gossip\n"
		// fmt.Println("current status : gossip")
		receiverList := SelectRandomProcess(K, node)
		for _, receiver := range receiverList {
			membership := currList.List[receiver]
			SendMessageToOne(node, membership.ID.IPAddress, portNum, false)
		}

	}
	return log
}

func SelectRandomProcess(k int, node nd.Node) []int {
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

	for {
		if len(list) >= k || len(list) == 0 {
			return list
		}
		randomNumber := rand.Int() % len(list)
		list = append(list[:randomNumber], list[randomNumber+1:]...)
	}
}

func Ping(conn *net.UDPConn, memberships ms.MsList, IsInitialization bool) ms.MsList {
	message := Packet{memberships, IsInitialization}
	encodedMessage := EncodeJSON(message)
	n, err := conn.Write(encodedMessage)

	CheckError(err)

	if !IsInitialization {
		return ms.MsList{}
	}

	var response [5120]byte
	var decodedResponse Packet
	n, err = conn.Read(response[0:])
	decodedResponse = DecodeJSON(response[:n])
	return decodedResponse.Input
}

// send membershipList to one processor
func SendMessageToOne(node nd.Node, targetIP string, portNum int, IsInitialization bool) ms.MsList {
	targetServicee := targetIP + ":" + strconv.Itoa(portNum)
	udpAddr, err := net.ResolveUDPAddr("udp4", targetServicee)
	CheckError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	CheckError(err)
	received := Ping(conn, node.MsList, IsInitialization)

	return received
}

// Listen to incoming messages (membershipList)
func ListenOnPort(conn *net.UDPConn, isIntroducer bool, node nd.Node, ATApointer *bool) (ms.MsList, string) {
	var portLog string
	var buf [5120]byte
	n, addr, err := conn.ReadFromUDP(buf[0:])
	if err != nil {
		fmt.Println("err != nil")
		return ms.MsList{}, ""
	}

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

	var message Packet
	message = DecodeJSON(buf[:n])

	if isIntroducer && message.IsInitialization { // server is introducer and message is an initialization message
		currMsList := node.MsList
		currMsList = currMsList.Add(message.Input.List[0], node.LocalTime)
		encodedMsg := EncodeJSON(Packet{currMsList, false})
		conn.WriteToUDP([]byte(encodedMsg), addr)
		return currMsList, portLog
	} else { // server is introducer but message is not an initialization message
		return message.Input, portLog
	}
}

func EncodeJSON(message Packet) []byte {
	encodedMessage, err := json.Marshal(message)
	CheckError(err)
	return encodedMessage
}

func DecodeJSON(encodedMessage []byte) Packet {
	var decodedMessage Packet
	err := json.Unmarshal(encodedMessage, &decodedMessage)
	CheckError(err)
	return decodedMessage
}

// func GenerateID(IPAddress string) string {
// 	return node.Id.IPAddress
// }

func CheckError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
