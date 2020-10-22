package FileSys

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	// "sort"
	"strconv"

	// nd"../Node"

	ms "../Membership"
	nd "../Node"
	pk "../Packet"
)

const BUFFERSIZE = 65400

// copied from https://opensource.com/article/18/6/copying-files-go
func copy(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

/*
	GetFileList(processNodePtr)
	Invoked when "ls sdfsfilename" has commanded

	return  <FileList  map[string][]ms.Id > stored in the leader process
*/
func GetFileList(processNodePtr *nd.Node) map[string][]ms.Id {

	if (*processNodePtr.IsLeaderPtr) == true {
		return processNodePtr.LeaderPtr.FileList
	}
	leaderService := *processNodePtr.LeaderServicePtr
	udpAddr, err := net.ResolveUDPAddr("udp4", leaderService)
	checkError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	checkError(err)

	_, err = conn.Write(pk.EncodePacket("get filelist", nil))

	var buf [4096]byte
	n, err := conn.Read(buf[0:])
	checkError(err)
	receivedPacket := pk.DecodePacket(buf[0:n])

	// target processes to store replicas
	FileList := pk.DecodeFileList(receivedPacket).FileList

	return FileList

}

// send the list of distributed files to the leader
func SendFilelist(processNodePtr *nd.Node) {
	leaderService := *processNodePtr.LeaderServicePtr
	udpAddr, err := net.ResolveUDPAddr("udp4", leaderService)
	checkError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	checkError(err)

	var buf [512]byte
	_, err = conn.Read(buf[0:])
	checkError(err)

	for _, filename := range *(*processNodePtr).DistributedFilesPtr {
		putPacket := pk.EncodePut(pk.Putpacket{processNodePtr.Id, filename})
		_, err := conn.Write(pk.EncodePacket("updateFileList", putPacket))
		checkError(err)

		_, err = conn.Read(buf[0:])
		checkError(err)
	}
}

func FailPut() {

}

// Put(processNodePtr *nd.Node, filename string, N int)
/*
	put a file to a distributed file system.

	Pick N other processors to store its replica
*/

func Put(processNodePtr *nd.Node, filename string, N int) {
	var idList []ms.Id

	//fmt.Println("PUT--------------------------")
	myID := (*processNodePtr).Id

	// local_files -> distributed_files
	from := processNodePtr.LocalPath + filename
	to := processNodePtr.DistributedPath + filename
	_, err := copy(from, to)
	checkError(err)

	*processNodePtr.DistributedFilesPtr = append(*processNodePtr.DistributedFilesPtr, filename)

	leaderService := *processNodePtr.LeaderServicePtr
	udpAddr, err := net.ResolveUDPAddr("udp4", leaderService)
	checkError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	checkError(err)

	packet := pk.EncodeIdList(pk.IdListpacket{N, myID, []ms.Id{}, filename})
	_, err = conn.Write(pk.EncodePacket("ReplicaList", packet))
	checkError(err)

	var buf [512]byte
	n, err := conn.Read(buf[0:])
	checkError(err)
	receivedPacket := pk.DecodePacket(buf[0:n])

	// target processes to store replicas
	idList = pk.DecodeIdList(receivedPacket).List

	Send(processNodePtr, filename, idList)
	// for i, id := range idList {
	// 	fmt.Println("picked desination:", i)
	// 	id.Print()

	// 	RequestTCP("put", id.IPAddress, filename, processNodePtr, id)
	// }

	putPacket := pk.EncodePut(pk.Putpacket{myID, filename})
	_, err = conn.Write(pk.EncodePacket("updateFileList", putPacket))
	checkError(err)

	_, err = conn.Read(buf[0:])
	checkError(err)

	fmt.Println("Put Done")
}

func Send(processNodePtr *nd.Node, filename string, idList []ms.Id) {
	for _, id := range idList {
		//fmt.Println("picked desination:", i)
		id.Print()

		RequestTCP("put", id.IPAddress, filename, processNodePtr, id)
	}
}

func SendDtoD(processNodePtr *nd.Node, filename string, idList []ms.Id) {
	for _, id := range idList {
		//fmt.Println("picked desination:", i)
		id.Print()

		RequestTCP("put", id.IPAddress, filename, processNodePtr, id)
	}
}

func Pull(processNodePtr *nd.Node, filename string, N int) {
	fmt.Println("PULL---------------")
	myID := processNodePtr.Id

	leaderService := *processNodePtr.LeaderServicePtr
	udpAddr, err := net.ResolveUDPAddr("udp4", leaderService)
	checkError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	checkError(err)

	packet := pk.EncodeIdList(pk.IdListpacket{N, myID, []ms.Id{}, filename})
	_, _ = conn.Write(pk.EncodePacket("FileNodeList", packet))
	checkError(err)

	var buf [512]byte
	n, err := conn.Read(buf[0:])
	checkError(err)
	receivedPacket := pk.DecodePacket(buf[0:n])

	// target processes to store replicas
	idList := pk.DecodeIdList(receivedPacket).List

	for _, id := range idList {
		if RequestTCP("fetch", id.IPAddress, filename, processNodePtr, id) {
			break
		}
	}
	fmt.Println("pull Done")
}

//SERVER
func ListenTCP(request string, fileName string, processNodePtr *nd.Node, connection *net.UDPConn, addr *net.UDPAddr) {
	//fmt.Println("ListenTCP----------------")

	var server net.Listener
	var err error
	ipaddr := processNodePtr.SelfIP
	service := ipaddr + ":" + "1288"
	//LOCAL
	// if processNodePtr.VmNum == 1 {
	// 	server, err = net.Listen("tcp", "localhost:1236")
	// } else {
	// 	server, err = net.Listen("tcp", "localhost:1237")
	// }

	//VM

	server, err = net.Listen("tcp", service)
	checkError(err)

	encodedMsg := pk.EncodePacket("Server opened", nil)
	connection.WriteToUDP(encodedMsg, addr)

	if err != nil {
		fmt.Println("Error listetning: ", err)
		os.Exit(1)
	}

	defer server.Close()
	//fmt.Println("Server started! Waiting for connections...")
	for {
		connection, err := server.Accept()
		if err != nil {
			fmt.Println("Error: ", err)
			os.Exit(1)
		}
		//fmt.Println("Client connected")

		if request == "put" {
			ReceiveFile(connection, processNodePtr.DistributedPath, processNodePtr)
			break
		} else if request == "fetch" {
			SendFile(connection, fileName, processNodePtr.DistributedPath)
			break
		}
	}
}

// CLIENT
func RequestTCP(command string, ipaddr string, fileName string, processNodePtr *nd.Node, id ms.Id) bool {
	// connect to server
	//fmt.Println("RequestTCP----------------")

	var service string

	//Local
	// if processNodePtr.VmNum == 1 {
	// 	service = ipaddr + ":" + "1237" //portnum
	// } else {
	// 	service = ipaddr + ":" + "1236" //portnum
	// }

	//VM
	service = ipaddr + ":" + "1288"
	OpenTCP(processNodePtr, command, fileName, id)

	connection, err := net.Dial("tcp", service)
	if err != nil {
		panic(err)
	}
	defer connection.Close()
	//fmt.Println("Connected, start processing request")

	check := false
	if command == "put" {
		fmt.Println("put")
		SendFile(connection, fileName, processNodePtr.DistributedPath)
	} else if command == "fetch" {

		fmt.Println("fetch")
		check = ReceiveFile(connection, "local_files", nil)
	}
	//fmt.Println("Request TCP Done")
	if check {
		return true
	}
	return false
}

// send / receive file
func SendFile(connection net.Conn, requestedFileName string, path string) {
	//fmt.Println("A server has connected!")
	defer connection.Close()

	file, err := os.Open(path + requestedFileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return
	}

	fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
	fileName := fillString(fileInfo.Name(), 64)
	//fmt.Println("Sending filename and filesize!")
	connection.Write([]byte(fileSize))
	connection.Write([]byte(fileName))
	sendBuffer := make([]byte, BUFFERSIZE)
	//fmt.Println("Start sending file!")
	for {
		_, err = file.Read(sendBuffer)
		if err == io.EOF {
			break
		}
		connection.Write(sendBuffer)
	}
	fmt.Println("File has been sent, closing connection!")
	return
}

func ReceiveFile(connection net.Conn, path string, processNodePtr *nd.Node) bool {
	defer connection.Close()

	//fmt.Println("----------------------------")
	//fmt.Println("receiving file...")

	bufferFileName := make([]byte, 64)
	bufferFileSize := make([]byte, 10)

	connection.Read(bufferFileSize)
	fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)

	connection.Read(bufferFileName)
	fileName := strings.Trim(string(bufferFileName), ":")

	//fmt.Println("create new file")
	newFile, err := os.Create(path + fileName)

	if err != nil {
		panic(err)
	}
	defer newFile.Close()
	var receivedBytes int64

	for {
		if (fileSize - receivedBytes) < BUFFERSIZE {
			io.CopyN(newFile, connection, (fileSize - receivedBytes))
			connection.Read(make([]byte, (receivedBytes+BUFFERSIZE)-fileSize))
			break
		}
		io.CopyN(newFile, connection, BUFFERSIZE)
		receivedBytes += BUFFERSIZE
	}
	fmt.Println("Received file completely!")

	// if received file is stored into distributed file system
	// alert leader of this udpate
	if path == processNodePtr.DistributedPath {
		UpdateLeader(fileName, processNodePtr)
	}

	//fmt.Println("updateLeader sent")
	return true
}

func fillString(retunString string, toLength int) string {
	for {
		lengtString := len(retunString)
		if lengtString < toLength {
			retunString = retunString + ":"
			continue
		}
		break
	}
	return retunString
}

func UpdateLeader(fileName string, processNodePtr *nd.Node) {

	if *processNodePtr.IsLeaderPtr {
		processNodePtr.LeaderPtr.FileList[fileName] = append(processNodePtr.LeaderPtr.FileList[fileName], processNodePtr.Id)
		processNodePtr.LeaderPtr.IdList[processNodePtr.Id] = append(processNodePtr.LeaderPtr.IdList[processNodePtr.Id], fileName)
	} else {
		//fmt.Println("UpdateLeader-------")
		myID := processNodePtr.Id
		//fromPath := (*processNodePtr).LocalPath
		//toPath := (*processNodePtr).DistributedPath

		leaderService := *processNodePtr.LeaderServicePtr
		udpAddr, err := net.ResolveUDPAddr("udp4", leaderService)
		checkError(err)

		conn, err := net.DialUDP("udp", nil, udpAddr)
		checkError(err)

		putPacket := pk.EncodePut(pk.Putpacket{myID, fileName})
		_, err = conn.Write(pk.EncodePacket("updateFileList", putPacket))
		checkError(err)

		var response [128]byte
		_, err = conn.Read(response[0:])
		checkError(err)
	}
}

func OpenTCP(processNodePtr *nd.Node, command string, filename string, id ms.Id) {

	service := id.IPAddress

	//Local
	// if processNodePtr.VmNum == 1 {
	// 	service += ":1235"
	// } else {
	// 	service += ":1234"
	// }

	//VM
	service += ":1234"

	udpAddr, err := net.ResolveUDPAddr("udp4", service)
	checkError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	checkError(err)

	packet := pk.EncodeTCPcmd(pk.TCPcmd{command, filename})
	_, err = conn.Write(pk.EncodePacket("openTCP", packet))
	checkError(err)

	var response [128]byte
	_, err = conn.Read(response[0:])
	checkError(err)
}

func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
