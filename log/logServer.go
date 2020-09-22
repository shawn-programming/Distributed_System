/*
	Sources:
	https://yourbasic.org/golang/log-to-file/
	http://tumregels.github.io/Network-Programming-with-Go/socket/udp_datagrams.html

*/
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	"./config"
)

func main() {

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s Need a Procces Number", os.Args[0])
		os.Exit(1)
	}
	ProcessStr := os.Args[1]
	// var f *os.File
	// file := createLogFile(ProcessStr)
	// f = &file

	PortList, err := config.Port()
	checkError(err)

	Process, err := strconv.Atoi(ProcessStr)
	checkError(err)

	PortNum := PortList[Process]

	fmt.Println("Portnum:", ProcessStr)

	service := ":" + PortNum

	udpAddr, err := net.ResolveUDPAddr("udp4", service)
	checkError(err)

	conn, err := net.ListenUDP("udp", udpAddr)
	checkError(err)

	f, err := os.OpenFile(ProcessStr+".log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	logger := log.New(f, "prefix", log.LstdFlags)
	for {
		handleClient(conn, "Process "+ProcessStr, logger)
	}
}

func handleClient(conn *net.UDPConn, msg string, logger *log.Logger) {

	var buf [512]byte

	n, addr, err := conn.ReadFromUDP(buf[0:])
	if err != nil {
		return
	}

	receivedMsg := string(buf[0:n])

	fmt.Println(receivedMsg)
	logger.Println(receivedMsg)

	conn.WriteToUDP([]byte(msg), addr)
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error %s", err.Error())
		os.Exit(1)
	}
}
