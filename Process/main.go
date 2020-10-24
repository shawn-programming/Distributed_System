package main

import (
	"fmt"
	"net"
	"os"
	"strconv"

	config "../logSystem/config"
	logger "./Logger"
	ms "./Membership"
	nd "./Node"
	sv "./Servent"
)

/*
	main()

	1) Fetch necessary data(IP address, PortNum, K value, etc...) from json file
	2) Create logs and initialize server
	3) Listens to the assigned Port and updates the membership List
	   Sends Data Packets containing its membership List information
*/
func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s need a VM number", os.Args[0])
		os.Exit(1)
	}

	IsLeader := os.Args[1] == "1"
	ATA := true
	TotalByteSent := 0
	var InputList []ms.MsList
	var DistributedFiles []string
	IDAdresses, _ := config.IPAddress()
	Ports, _ := config.Port()
	LeaderService := IDAdresses[1] + ":" + strconv.Itoa(Ports[0])
	Initiator := ""

	processNode := nd.CreateNode(os.Args[1], &IsLeader, &ATA, &TotalByteSent, &InputList, &LeaderService, &DistributedFiles, &Initiator) // Processor's Node

	tempLeader := nd.Leader{&processNode.MsList, map[string][]ms.Id{}, map[ms.Id][]string{}}
	processNode.LeaderPtr = &tempLeader

	f, f2, f3 := logger.LoggerPointerInit(os.Args[1])
	defer f.Close()
	defer f2.Close()
	defer f3.Close()

	processNode.LeaderServicePtr = &LeaderService

	// write vm's info to the log
	LoggerPerSec, Logger, LoggerByte := logger.LoggerInitialization(f, f2, f3, processNode)
	processNode.LoggerPerSec = LoggerPerSec
	processNode.Logger = Logger
	processNode.LoggerByte = LoggerByte

	fmt.Println(" ================== open server and logging system ==================")
	processNode.LoggerPerSec.Println(" ================== open server and logging system ==================")

	// Myservice for communicating commands
	udpAddr, err := net.ResolveUDPAddr("udp4", processNode.MyService)
	sv.CheckError(err)

	conn, err := net.ListenUDP("udp", udpAddr)
	sv.CheckError(err)

	// service for heartbeat communication
	udpAddr, err = net.ResolveUDPAddr("udp4", processNode.SelfIP+":"+strconv.Itoa(processNode.MyPortNumHB))
	sv.CheckError(err)

	connHB, err := net.ListenUDP("udp", udpAddr)
	sv.CheckError(err)

	// open the server and collect msgs from other processors
	processNode.LoggerPerSec.Println("-------starting listening----------")
	go sv.OpenServer(conn, &processNode)

	go sv.OpenHeartbeat(connHB, &processNode)

	if !processNode.IsIntroducer {
		sv.NewMemberInitialization(&processNode)
	}

	/* special command (-h to see the list) */
	go sv.GetCommand(&processNode)

	// Update current membership List and sends its information to other members
	processNode.LoggerPerSec.Println("----------Start Sending----------")
	go sv.Heartbeat(&processNode)

	for {
		// hand the system
	}
}
