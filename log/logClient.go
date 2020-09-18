/* UDPDaytimeClient
 */
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"./config"
)

func main() {
	// if len(os.Args) != 3 {
	// 	fmt.Fprintf(os.Stderr, "Usage: %s host:port processID", os.Args[0])
	// 	os.Exit(1)
	// }
	hostList, err := config.IPAddress()
	checkError(err)

	host := hostList[0]

	PortList, err := config.Port()
	checkError(err)

	f, err := os.OpenFile("client.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	logger := log.New(f, "prefix", log.LstdFlags)

	connList := []*net.UDPConn{}

	wg := &sync.WaitGroup{}

	for i, s := range PortList {
		wg.Add(1)

		processID := s

		service := host + ":" + s
		udpAddr, err := net.ResolveUDPAddr("udp4", service)
		checkError(err)

		conn, err := net.DialUDP("udp", nil, udpAddr)
		connList = append(connList, conn)
		checkError(err)

		go ping(processID, connList[i], wg, logger)
	}

	wg.Wait()
}

func ping(processID string, conn *net.UDPConn, wg *sync.WaitGroup, logger *log.Logger) {
	for i := 0; i < 10; i++ {
		_, err := conn.Write([]byte("Process: " + processID))
		checkError(err)

		var buf [512]byte
		n, err := conn.Read(buf[0:])
		checkError(err)

		receivedMsg := string(buf[0:n])
		fmt.Println(receivedMsg)
		logger.Println(receivedMsg)

		time.Sleep(time.Second * 2)
	}
	wg.Done()
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error %s", err.Error())
		os.Exit(1)
	}
}
