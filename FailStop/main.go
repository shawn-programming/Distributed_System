package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"../logSystem/config"
	ms "./Membership"
	nd "./Node"
	sv "./servent"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s need a VM number", os.Args[0])
		os.Exit(1)
	}
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

	// myPortNum := portList[0]
	// destPortNum := portList[0]

	myPortNum := portList[(vmNum+1)%2]
	destPortNum := portList[vmNum%2]

	myService := selfIP + ":" + strconv.Itoa(myPortNum)
	serverID := selfIP + "_" + string(time.Now().Format(time.RFC1123)) // default value for the introducer
	processNode := nd.CreateNode(serverID, selfIP, 0, timeOut)

	f, err := os.OpenFile("./vm_"+vmNumStr+"_per_sec.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	f2, err := os.OpenFile("./vm_"+vmNumStr+".log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}
	defer f2.Close()

	loggerPerSec := log.New(f, "Processor_"+vmNumStr, log.LstdFlags)
	logger := log.New(f2, "Processor_"+vmNumStr, log.LstdFlags)

	fmt.Println("ServerID:", serverID)
	loggerPerSec.Println("ServerID:", serverID)
	logger.Println("ServerID:", serverID)

	fmt.Println("selfIP:", myService)
	loggerPerSec.Println("selfIP:", myService)
	logger.Println("selfIP:", myService)

	fmt.Println("TimeOut:", timeOut)
	loggerPerSec.Println("TimeOut:", timeOut)
	logger.Println("TimeOut:", timeOut)

	scanner := bufio.NewScanner(os.Stdin)
	ATA := true

	go func() {
		for {
			scanner.Scan()
			command := scanner.Text()

			if command == "gossip" {
				fmt.Println("Changing to Gossip")
				loggerPerSec.Println("Changing to Gossip")
				logger.Println("Changing to Gossip")
				ATA = false
				sv.PingMsg(processNode, "gossip", destPortNum)
			} else if command == "ata" {
				fmt.Println("Changing to ATA")
				ATA = true
				sv.PingMsg(processNode, "ata", destPortNum)
				loggerPerSec.Println("Changing to ATA")
				logger.Println("Changing to ATA")
			} else if command == "leave" {
				fmt.Println("(Leave)Terminating vm_", vmNumStr)
				loggerPerSec.Println("(Leave)Terminating vm_" + vmNumStr)
				logger.Println("(Leave)Terminating vm_" + vmNumStr)
				os.Exit(1)
			} else if command == "memberlist" {
				fmt.Println("\nMembership List: \n" + processNode.MsList.PrintLog())
				loggerPerSec.Println("\nMembership List: \n" + processNode.MsList.PrintLog())
				logger.Println("\nMembership List: \n" + processNode.MsList.PrintLog())
			} else if command == "id" {
				fmt.Println("Current IP and port:", myService)
				loggerPerSec.Println("\nCurrent IP and port: " + myService + "\n")
				logger.Println("\nCurrent IP and port:: " + myService + "\n")
			} else {
				fmt.Println("Invalid Command")
			}
		}
	}()

	fmt.Println(" ================== open server and logging system ==================")
	loggerPerSec.Println(" ================== open server and logging system ==================")

	udpAddr, err := net.ResolveUDPAddr("udp4", myService)
	sv.CheckError(err)

	conn, err := net.ListenUDP("udp", udpAddr)
	sv.CheckError(err)

	// if newly joined introducer, notify the introducer and update its membership List
	if !isIntroducer {
		fmt.Println("Connecting to Introducer...")
		loggerPerSec.Println("Connecting to Introducer...")
		logger.Println("Connecting to Introducer...")

		received := sv.SendMessageToOne(processNode, IntroducerIP, destPortNum, true)
		processNode.MsList = received
		fmt.Println("Connected!")
		loggerPerSec.Println("Connected!")
		logger.Println("Connected!")
	}

	// a placeholder for input membership List from other processors
	var logStr string
	InputList := []ms.MsList{}
	newList := InputList

	// fmt.Println("-------starting listening----------")
	loggerPerSec.Println("-------starting listening----------")
	go func(conn *net.UDPConn, isIntroducer bool, processNode nd.Node) {
		for {
			tempList, portLog := sv.ListenOnPort(conn, isIntroducer, processNode, &ATA)
			InputList = append(InputList, tempList)
			if len(portLog) > 0 {
				logger.Println(portLog)
			}
		}
	}(conn, isIntroducer, processNode)

	// fmt.Println("----------Start Sending----------")
	loggerPerSec.Println("----------Start Sending----------")

	for {
		newList = InputList
		InputList = []ms.MsList{}
		processNode, logStr = processNode.IncrementLocalTime(newList)
		if logStr != "" {
			loggerPerSec.Println(logStr)
			logger.Println(logStr)
		}
		logPerSec := sv.PingToOtherProcessors(destPortNum, processNode, ATA, K)
		loggerPerSec.Println(logPerSec)

	}

}
