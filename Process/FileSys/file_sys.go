package FileSys

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"time"

	// "sort"
	"strconv"

	// nd"../Node"

	ms "../Membership"
	nd "../Node"
	pk "../Packet"
)

const BUFFERSIZE = 65400

/*
Remove(nodePtr *nd.Node, filename string)

remove a file <filename> from the distributed folder
and also remove it from the DistributedFilesPtr
*/
func Remove(nodePtr *nd.Node, filename string) {
	error := os.Remove(nodePtr.DistributedPath + filename)
	checkError(error)
	for i, file := range *nodePtr.DistributedFilesPtr {
		if filename == file {
			*nodePtr.DistributedFilesPtr = append((*nodePtr.DistributedFilesPtr)[:i], (*nodePtr.DistributedFilesPtr)[i+1:]...)
			fmt.Println("Removed", filename)
			return
		}
	}
	fmt.Println("Error! file not found")
}

func RemoveFile(nodePtr *nd.Node, filename string) {
	leaderService := *nodePtr.LeaderServicePtr

	// if the processor is not the leader, request the leader to distribute the messages
	if leaderService != nodePtr.MyService {
		udpAddr, err := net.ResolveUDPAddr("udp4", leaderService)
		checkError(err)

		conn, err := net.DialUDP("udp", nil, udpAddr)
		checkError(err)

		// send the leader about the remove request
		_, err = conn.Write(pk.EncodePacket("Remove", []byte(filename)))

		var buf [4096]byte
		n, err := conn.Read(buf[0:])
		checkError(err)
		receivedPacket := pk.DecodePacket(buf[0:n])
		fmt.Println(receivedPacket.Ptype)
	} else { // if the processor is the leader, DIY
		fileOwners, exists := nodePtr.LeaderPtr.FileList[filename]
		//udate filelist
		delete(nodePtr.LeaderPtr.FileList, filename)

		//update idlist
		for id, filelist := range nodePtr.LeaderPtr.IdList {
			for i, file := range filelist {
				if file == filename {
					(*nodePtr.LeaderPtr).IdList[id] = append((*nodePtr.LeaderPtr).IdList[id][:i], (*nodePtr.LeaderPtr).IdList[id][i+1:]...)
				}
			}
		}

		if exists {
			fmt.Println("File Found and removed it")
		} else {
			fmt.Println("File not found")
		}

		// make each file owner to remove the file
		for _, fileOwner := range fileOwners {
			Service := fileOwner.IPAddress + ":" + strconv.Itoa(nodePtr.DestPortNum)
			if Service == nodePtr.MyService { // if leader have the file, remove it
				Remove(nodePtr, filename)
			} else {
				udpAddr, err := net.ResolveUDPAddr("udp4", Service)
				checkError(err)
				conn, err := net.DialUDP("udp", nil, udpAddr)
				checkError(err)

				_, err = conn.Write(pk.EncodePacket("RemoveFile", []byte(filename)))
				checkError(err)
				var buf [512]byte
				_, err = conn.Read(buf[0:])
				checkError(err)
			}
		}
	}
}

/*
copy(src, dst string)

copy a file from the source to the destination
// code copied from https://opensource.com/article/18/6/copying-files-go
*/
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

	// fmt.Println("Send File List's Service:", leaderService)

	for _, filename := range *(*processNodePtr).DistributedFilesPtr {
		// fmt.Println("sending:", filename)
		putPacket := pk.EncodePut(pk.Putpacket{processNodePtr.Id, filename})
		_, err := conn.Write(pk.EncodePacket("updateFileList", putPacket))
		checkError(err)

		_, err = conn.Read(buf[0:])
		checkError(err)
		// fmt.Println("seding done")
	}
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

	// request leader about the destinations to send the replica
	packet := pk.EncodeIdList(pk.IdListpacket{N, myID, []ms.Id{}, filename})
	_, err = conn.Write(pk.EncodePacket("ReplicaList", packet))
	checkError(err)

	var buf [512]byte
	n, err := conn.Read(buf[0:])
	checkError(err)
	receivedPacket := pk.DecodePacket(buf[0:n])

	// target processes to store replicas
	idList = pk.DecodeIdList(receivedPacket).List

	// send file replica to the idLists
	Send(processNodePtr, filename, idList)

	putPacket := pk.EncodePut(pk.Putpacket{myID, filename})
	_, err = conn.Write(pk.EncodePacket("updateFileList", putPacket))
	checkError(err)

	_, err = conn.Read(buf[0:])
	checkError(err)

	// fmt.Println("Put Done")
}

func Send(processNodePtr *nd.Node, filename string, idList []ms.Id) {
	for _, id := range idList {
		//fmt.Println("picked desination:", i)
		// fmt.Println("Sending to")
		// id.Print()
		RequestTCP("put", id.IPAddress, filename, processNodePtr, id)
	}
}

func Pull(processNodePtr *nd.Node, filename string, N int) {
	// fmt.Println("PULL---------------")

	files, err := ioutil.ReadDir(processNodePtr.DistributedPath)
	checkError(err)

	// if the file is inside the distributed folder of the process, just move it
	for _, file := range files {
		if file.Name() == filename {
			src := processNodePtr.DistributedPath + filename
			dest := processNodePtr.LocalPath + filename
			copy(src, dest)
			fmt.Println("Received a file:", filename)
			return
		}
	}

	myID := processNodePtr.Id

	leaderService := *processNodePtr.LeaderServicePtr

	// process is not the leader, send a request to the leader
	if *processNodePtr.IsLeaderPtr == false {
		udpAddr, err := net.ResolveUDPAddr("udp4", leaderService)
		checkError(err)

		conn, err := net.DialUDP("udp", nil, udpAddr)
		checkError(err)

		packet := pk.EncodeTCPsend(pk.TCPsend{[]ms.Id{myID}, filename})
		_, _ = conn.Write(pk.EncodePacket("request", packet))
		checkError(err)

		var buf [512]byte
		n, err := conn.Read(buf[0:])
		checkError(err)
		receivedPacket := pk.DecodePacket(buf[0:n])
		fmt.Println(receivedPacket.Ptype)
	} else { // process is the leader, DIY
		destinations := []ms.Id{myID}
		fileOwners, exists := processNodePtr.LeaderPtr.FileList[filename]
		from := fileOwners[0]
		Service := from.IPAddress + ":" + strconv.Itoa(processNodePtr.DestPortNum)

		if !exists {
			fmt.Println(filename + " is not found in the system")
		} else {
			// fmt.Println("telling DFs to send a file to you...", nil)
			udpAddr, err := net.ResolveUDPAddr("udp4", Service)
			checkError(err)
			conn, err := net.DialUDP("udp", nil, udpAddr)
			checkError(err)
			packet := pk.EncodeTCPsend(pk.TCPsend{destinations, filename})
			_, err = conn.Write(pk.EncodePacket("send", packet))
			checkError(err)
			var buf [512]byte
			_, err = conn.Read(buf[0:])
			checkError(err)
		}
	}

	// fmt.Println("pull Done")
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

	// fmt.Println("Sever Opened")

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
			// fmt.Println("receive file")
			ReceiveFile(connection, processNodePtr.DistributedPath, processNodePtr)
			break
		} else if request == "fetch" {
			// fmt.Println("sendfile")
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
	// fmt.Println("OpenTCP")
	OpenTCP(processNodePtr, command, fileName, id)
	// fmt.Println("OpenTCP Done")

	connection, err := net.Dial("tcp", service)
	if err != nil {
		panic(err)
	}
	defer connection.Close()
	// fmt.Println("Connected, start processing request")

	check := false
	if command == "put" {
		// fmt.Println("put")
		SendFile(connection, fileName, processNodePtr.DistributedPath)
	} else if command == "fetch" {

		// fmt.Println("fetch")
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
	connection.Write([]byte(fileSize))
	connection.Write([]byte(fileName))
	sendBuffer := make([]byte, BUFFERSIZE)
	// fmt.Println("Start sending file!")
	for {
		_, err = file.Read(sendBuffer)
		if err == io.EOF {
			break
		}
		connection.Write(sendBuffer)
	}
	fmt.Println("File has been sent")
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
	fmt.Println("Received file:", fileName)

	// if received file is stored into distributed file system
	// alert leader of this udpate
	if path == processNodePtr.DistributedPath {
		*processNodePtr.DistributedFilesPtr = append(*processNodePtr.DistributedFilesPtr, fileName)
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
	// fmt.Println("OpenTCP net dialed")

	packet := pk.EncodeTCPcmd(pk.TCPcmd{command, filename})
	_, err = conn.Write(pk.EncodePacket("openTCP", packet))
	checkError(err)

	// fmt.Println("openTCP write done")
	var response [128]byte
	_, err = conn.Read(response[0:])

	// fmt.Println("openTCP read done")

	checkError(err)
}

func LeaderInit(node *nd.Node, failedLeader string) {
	time.Sleep(time.Second * 5) // 5 == timeOut
	members := node.AliveMembers()
	*node.IsLeaderPtr = true
	for _, member := range members {
		Service := member.ID.IPAddress + ":" + strconv.Itoa(node.DestPortNum)
		if Service == failedLeader || Service == node.MyService {
			continue
		}
		// fmt.Println("file list receive start")
		udpAddr, err := net.ResolveUDPAddr("udp4", Service)
		checkError(err)
		conn, err := net.DialUDP("udp", nil, udpAddr)
		checkError(err)

		_, err = conn.Write(pk.EncodePacket("send a filelist", nil))
		checkError(err)

		var buf [512]byte
		_, err = conn.Read(buf[0:])
		checkError(err)

		// fmt.Println("file list received from", Service)
	}

	// fmt.Println("store info about df")
	// store the info about its distributed files
	for _, file := range *node.DistributedFilesPtr {
		node.LeaderPtr.FileList[file] = append(node.LeaderPtr.FileList[file], node.Id)
		node.LeaderPtr.IdList[node.Id] = append(node.LeaderPtr.IdList[node.Id], file)
	}

	// fmt.Println("store info about df done")

	for file, list := range node.LeaderPtr.FileList {
		// fmt.Println("Checking file", file)
		fmt.Println("File ", file, "is stored in the following Addresses:")
		for i, ID := range list {
			fmt.Println("	", i, ":", ID.IPAddress)
		}
		if len(list) < node.MaxFail+1 {
			fileOwners := node.LeaderPtr.FileList[file]
			fmt.Println(file)

			N := node.MaxFail - len(fileOwners) + 1

			destinations := node.PickReplicas(N, fileOwners)
			from := fileOwners[0]

			Service := from.IPAddress + ":" + strconv.Itoa(node.DestPortNum)
			// fmt.Println("Service: ", Service)

			if Service == node.MyService { // if the sender is the current node (Leader)
				// fmt.Println("sender is current node")
				Send(node, file, destinations)
				// fmt.Println("sender is current node done")

			} else {
				// fmt.Println("sender is NOT current node")
				udpAddr, err := net.ResolveUDPAddr("udp4", Service)
				checkError(err)
				conn, err := net.DialUDP("udp", nil, udpAddr)
				checkError(err)
				packet := pk.EncodeTCPsend(pk.TCPsend{destinations, file})
				_, err = conn.Write(pk.EncodePacket("send", packet))
				checkError(err)
				var buf [512]byte
				_, err = conn.Read(buf[0:])
				// fmt.Println("Received Ack")
				checkError(err)
			}
			// fmt.Println("number of", file, "replica is balanced now")
		}

		// fmt.Println(file, "list updated")
	}

	fmt.Println("Leader Init completed")
}

func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
