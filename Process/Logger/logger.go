package logger

import (
	"fmt"
	"log"
	"os"

	nd "../Node"
)

/*
LoggerPointerInit
	declar logger pointers
	RETURN: logger pointers
*/
func LoggerPointerInit(vmNumStr string) (*os.File, *os.File, *os.File) {
	f, err := os.OpenFile("./vm_"+vmNumStr+"_per_sec.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}

	// log keeps track of special info
	f2, err := os.OpenFile("./vm_"+vmNumStr+".log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}

	// log keeps track of special info
	f3, err := os.OpenFile("./vm_"+vmNumStr+"_byte.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}

	return f, f2, f3
}

/*
LoggerInitialization
	write vm's initial info to the log
	RETURN: loggers
*/
func LoggerInitialization(f, f2, f3 *os.File, node nd.Node) (*log.Logger, *log.Logger, *log.Logger) {
	vmNumStr := node.VmNumStr
	serverID := node.ServerID
	myService := node.MyService
	timeOut := node.TimeOut
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

	return loggerPerSec, logger, loggerByte
}
