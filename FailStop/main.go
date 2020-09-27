package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"../logSystem/config"
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

	K, _ := config.K()                     // K value for gossip
	vmNum, err := strconv.Atoi(os.Args[1]) // VM number
	vmNumStr := os.Args[1]
	IntroducerIPList, _ := config.IPAddress() // Introducer's IP
	IntroducerIP := IntroducerIPList[1]
	portList, _ := config.Port() // Port number's list
	// portNum := portList[vmNum]
	timeOut, _ := config.TimeOut() // Time Out info
	isIntroducer := vmNum == 1     // True if the proceesor is an introducer, else False
	selfIP := IntroducerIPList[vmNum]
	// for VM test
	myPortNum := portList[0]   // Processor's port number
	destPortNum := portList[0] // Receiver's port number
	// for local test
	// myPortNum := portList[(vmNum+1)%2] // Processor's port number
	// destPortNum := portList[vmNum%2]   // Receiver's port number

	myService := selfIP + ":" + strconv.Itoa(myPortNum)                // processor's service for UDP
	serverID := selfIP + "_" + string(time.Now().Format(time.RFC1123)) // Processor's ID
	processNode := nd.CreateNode(serverID, selfIP, 0, timeOut)         // Processor's Node

	var TotalByteSent int // Keeps track of total number of bytes sent and received
	// log keeps track of every second
	f, err := os.OpenFile("./vm_"+vmNumStr+"_per_sec.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	// log keeps track of special info
	f2, err := os.OpenFile("./vm_"+vmNumStr+".log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}
	defer f2.Close()

	// log keeps track of special info
	f3, err := os.OpenFile("./vm_"+vmNumStr+"_byte.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}
	defer f3.Close()

	// write vm's info to the log
	loggerPerSec := log.New(f, "Processor_"+vmNumStr, log.LstdFlags)
	logger := log.New(f2, "Processor_"+vmNumStr, log.LstdFlags)
	loggerByte := log.New(f3, "Processor_"+vmNumStr, log.LstdFlags)

	fmt.Println("ServerID:", serverID)
	loggerPerSec.Println("ServerID:", serverID)
	logger.Println("ServerID:", serverID)

	fmt.Println("selfIP:", myService)
	loggerPerSec.Println("selfIP:", myService)
	logger.Println("selfIP:", myService)

	fmt.Println("TimeOut:", timeOut)
	loggerPerSec.Println("TimeOut:", timeOut)
	logger.Println("TimeOut:", timeOut)

	ATA := true

	fmt.Println(" ================== open server and logging system ==================")
	loggerPerSec.Println(" ================== open server and logging system ==================")

	udpAddr, err := net.ResolveUDPAddr("udp4", myService)
	sv.CheckError(err)

	conn, err := net.ListenUDP("udp", udpAddr)
	sv.CheckError(err)

	// a placeholder for input membership List from other processors
	var logStr string
	InputList := []ms.MsList{}
	newList := InputList
	// open the server and collect msgs from other processors
	loggerPerSec.Println("-------starting listening----------")
	go func(conn *net.UDPConn, isIntroducer bool, processNode nd.Node) {
		for {
			tempList, portLog := sv.ListenOnPort(conn, isIntroducer, processNode, &ATA, destPortNum)
			// update InputList to be used for IncrementLocalTime()
			InputList = append(InputList, tempList)
			if len(portLog) > 0 {
				logger.Println(portLog)
			}
		}
	}(conn, isIntroducer, processNode)

	// if newly joined introducer, notify the introducer and update its membership List
	if !isIntroducer {
		fmt.Println("Connecting to Introducer...")
		loggerPerSec.Println("Connecting to Introducer...")
		logger.Println("Connecting to Introducer...")

		received, byteSent := sv.SendMessageToOne(processNode, IntroducerIP, destPortNum, true)

		TotalByteSent += byteSent

		loggerByte.Println(string(time.Now().Format(time.RFC1123)))
		loggerByte.Println("Byte sent 		: " + strconv.Itoa(byteSent) + " Bytes.")
		loggerByte.Println("Total byte sent	: " + strconv.Itoa(TotalByteSent) + " Bytes.\n")

		processNode.MsList = received
		fmt.Println("Connected!")
		loggerPerSec.Println("Connected!")
		logger.Println("Connected!")
	}

	/*
		special command
			gossip:		change the system into a gossip heartbeating
			ata:		change the system into a All-to-All heartbeating
			leave: 		voluntarily leave the system. (halt)
			memberlist: print VM's memberlist to the terminal
			id:			print current IP address and assigned Port number
			-h: 		print list of commands
	*/
	go sv.GetCommand(&ATA, loggerByte, logger, loggerPerSec, &processNode, destPortNum, vmNumStr, myService)

	// Update current membership List and sends its information to other members
	loggerPerSec.Println("----------Start Sending----------")
	for {
		newList = InputList
		InputList = []ms.MsList{}

		// update the processor's membership list
		processNode, logStr = processNode.IncrementLocalTime(newList)
		if logStr != "" {
			loggerPerSec.Println(logStr)
			logger.Println(logStr)
		}

		// sned the processor's member to other processors
		logPerSec, byteSent := sv.PingToOtherProcessors(destPortNum, processNode, ATA, K)
		loggerPerSec.Println(logPerSec)

		TotalByteSent += byteSent

		if byteSent != 0 {
			loggerByte.Println(string(time.Now().Format(time.RFC1123)))
			loggerByte.Println("Byte sent 		: " + strconv.Itoa(byteSent) + " Bytes.")
			loggerByte.Println("Total byte sent	: " + strconv.Itoa(TotalByteSent) + " Bytes.\n")
		}

	}
}
