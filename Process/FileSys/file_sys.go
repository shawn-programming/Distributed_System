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

/*	Buffersize used for transfering file over tcp 	*/
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

/*
	RemoveFile(nodePtr *nd.Node, filename string)

	Invoked upon remove request
	request leader to command all nodes with the file to remove the file including leader itself.
*/
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
	//fmt.Println("get file list")

	if (*processNodePtr.IsLeaderPtr) == true {
		return processNodePtr.LeaderPtr.FileList
	}
	leaderService := *processNodePtr.LeaderServicePtr
	udpAddr, err := net.ResolveUDPAddr("udp4", leaderService)
	checkError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	checkError(err)

	_, err = conn.Write(pk.EncodePacket("get filelist", nil))

	var buf [4096 * 5]byte
	n, err := conn.Read(buf[0:])
	checkError(err)
	receivedPacket := pk.DecodePacket(buf[0:n])

	// target processes to store replicas
	FileList := pk.DecodeFileList(receivedPacket).FileList

	return FileList

}


/*
	SendFilelist(processNodePtr *nd.Node)

	send current node's filelist to the leader for update
*/
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


/*
	Put(processNodePtr *nd.Node, filename string, N int)
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
	// go func() {
	Send(processNodePtr, filename, idList, false)

	putPacket := pk.EncodePut(pk.Putpacket{myID, filename})
	_, err = conn.Write(pk.EncodePacket("updateFileList", putPacket))
	checkError(err)

	_, err = conn.Read(buf[0:])
	checkError(err)

	// }()

	//fmt.Println("Put Done")
}


/*
	Send(processNodePtr *nd.Node, filename string, idList []ms.Id, IsPull bool)

	initiates file transfer protocol to all nodes with id in idList
*/
func Send(processNodePtr *nd.Node, filename string, idList []ms.Id, IsPull bool) {
	for _, id := range idList {
		//fmt.Println("picked desination:", i)
		// fmt.Println("Sending to")
		// id.Print()

		RequestTCP("put", id.IPAddress, filename, processNodePtr, id, IsPull)
	}
}


/*
	Pull(processNodePtr *nd.Node, filename string, N int) 	

	request the leader to find the node with the file and commands the node
	to send the requested file to the client.
*/
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

		packet := pk.EncodeTCPsend(pk.TCPsend{[]ms.Id{myID}, filename, true})
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
			packet := pk.EncodeTCPsend(pk.TCPsend{destinations, filename, true})
			_, err = conn.Write(pk.EncodePacket("send", packet))
			checkError(err)
			var buf [512]byte
			_, err = conn.Read(buf[0:])
			checkError(err)
		}
	}

	// fmt.Println("pull Done")
}


/*
	ListenTCP(request string, fileName string, processNodePtr *nd.Node, connection *net.UDPConn, addr *net.UDPAddr, IsPull bool) {

	Upon receiving open TCP request, the server starts listening to its TCP port for incoming data
*/
func ListenTCP(request string, fileName string, processNodePtr *nd.Node, connection *net.UDPConn, addr *net.UDPAddr, IsPull bool) {
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
	for {
		connection, err := server.Accept()
		if err != nil {
			fmt.Println("Error: ", err)
			os.Exit(1)
		}
		if IsPull {
			ReceiveFile(connection, processNodePtr.LocalPath, processNodePtr)
			break
		} else {
			ReceiveFile(connection, processNodePtr.DistributedPath, processNodePtr)
			break
		}

	}
}


/*
	RequestTCP(command string, ipaddr string, fileName string, processNodePtr *nd.Node, id ms.Id, IsPull bool)

	initiates TCP procedure for sending file
*/
func RequestTCP(command string, ipaddr string, fileName string, processNodePtr *nd.Node, id ms.Id, IsPull bool) {

	var service string

	//Local
	// if processNodePtr.VmNum == 1 {
	// 	service = ipaddr + ":" + "1237" //portnum
	// } else {
	// 	service = ipaddr + ":" + "1236" //portnum
	// }

	//VM
	service = ipaddr + ":" + "1288"
	//fmt.Println("OpenTCP")
	OpenTCP(processNodePtr, command, fileName, id, IsPull)
	//fmt.Println("OpenTCP Done")

	connection, err := net.Dial("tcp", service)
	if err != nil {
		panic(err)
	}
	defer connection.Close()
	//fmt.Println("Connected, start processing request")

	if command == "put" {
		//fmt.Println("put")
		SendFile(connection, fileName, processNodePtr.DistributedPath)

	}

}

/*
	SendFile(connection net.Conn, requestedFileName string, path string) {

	transfer file over established tcp connection
*/
func SendFile(connection net.Conn, requestedFileName string, path string) {
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


/*
	ReceiveFile(connection net.Conn, path string, processNodePtr *nd.Node)

	receive file from established tcp connection
*/
func ReceiveFile(connection net.Conn, path string, processNodePtr *nd.Node) {
	defer connection.Close()

	//fmt.Println("receiving file...")

	bufferFileName := make([]byte, 64)
	bufferFileSize := make([]byte, 10)

	connection.Read(bufferFileSize)
	fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)

	connection.Read(bufferFileName)
	fileName := strings.Trim(string(bufferFileName), ":")

	fmt.Println("create new file, path:", path+fileName)
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
}

/*
	fillString(retunString string, toLength int) 

	used to fill string	
	code copied from https://mrwaggel.be/post/golang-transfer-a-file-over-a-tcp-socket/
*/
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


/*
	UpdateLeader(fileName string, processNodePtr *nd.Node)

	request leader to update its file meta-data
*/	
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


/*
	OpenTCP(processNodePtr *nd.Node, command string, filename string, id ms.Id, IsPull bool) {

	request node with the input id to listen to its TCP connection so that it can establish connection
*/
func OpenTCP(processNodePtr *nd.Node, command string, filename string, id ms.Id, IsPull bool) {

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

	packet := pk.EncodeTCPcmd(pk.TCPcmd{command, filename, IsPull})
	_, err = conn.Write(pk.EncodePacket("openTCP", packet))
	checkError(err)

	var response [128]byte
	_, err = conn.Read(response[0:])

	checkError(err)
}

/*
	LeaderInit(node *nd.Node, failedLeader string)

	upon new election of a leader, 
	it gathers necessary file meta-data to perform the role as a new leader, and
	accounts for possible failure during leader election by 
	checking number of files presetn in the DFS, and taking appropriate measure. 
*/
func LeaderInit(node *nd.Node, failedLeader string) {
	time.Sleep(time.Second * 5) // 5 == timeOut
	members := node.AliveMembers()
	*node.IsLeaderPtr = true

	for _, member := range members {
		Service := member.ID.IPAddress + ":" + strconv.Itoa(node.DestPortNum)
		if Service == failedLeader || Service == node.MyService {
			continue
		}
		udpAddr, err := net.ResolveUDPAddr("udp4", Service)
		checkError(err)
		conn, err := net.DialUDP("udp", nil, udpAddr)
		checkError(err)

		_, err = conn.Write(pk.EncodePacket("send a filelist", nil))
		checkError(err)

		var buf [512]byte
		n, err := conn.Read(buf[0:])
		packet := pk.DecodePacket(buf[:n])
		decodedPacket := pk.DecodeFilesPacket(packet)
		checkError(err)

		idInfo := decodedPacket.Id
		filenames := decodedPacket.FileName
		for _, filename := range filenames {
			node.LeaderPtr.FileList[filename] = append(node.LeaderPtr.FileList[filename], idInfo)

			// update IdList
			node.LeaderPtr.IdList[idInfo] = append(node.LeaderPtr.IdList[idInfo], filename)
		}
	}
	// for file, list := range node.LeaderPtr.FileList {
	// 	fmt.Println("File ", file, "is stored in the following Addresses:")
	// 	for i, ID := range list {
	// 		fmt.Println("	", i, ":", ID.IPAddress)
	// 	}
	// }

	// store the info about its distributed files
	for _, file := range *node.DistributedFilesPtr {
		node.LeaderPtr.FileList[file] = append(node.LeaderPtr.FileList[file], node.Id)
		node.LeaderPtr.IdList[node.Id] = append(node.LeaderPtr.IdList[node.Id], file)
	}

	for file, list := range node.LeaderPtr.FileList {

		if len(list) < node.MaxFail+1 {
			fileOwners := node.LeaderPtr.FileList[file]
			fmt.Println(file)

			N := node.MaxFail - len(fileOwners) + 1

			destinations := node.PickReplicas(N, fileOwners)
			from := fileOwners[0]

			Service := from.IPAddress + ":" + strconv.Itoa(node.DestPortNum)
			fmt.Println("Service: ", Service)

			if Service == node.MyService { // if the sender is the current node (Leader)
				Send(node, file, destinations, false)

			} else {
				udpAddr, err := net.ResolveUDPAddr("udp4", Service)
				checkError(err)
				conn, err := net.DialUDP("udp", nil, udpAddr)
				checkError(err)
				packet := pk.EncodeTCPsend(pk.TCPsend{destinations, file, false})
				_, err = conn.Write(pk.EncodePacket("send", packet))
				checkError(err)
				var buf [512]byte
				_, err = conn.Read(buf[0:])
				checkError(err)
			}
			//fmt.Println("number of", file, "replica is balanced now")
		}

		//fmt.Println(file, "list updated")
	}

	fmt.Println("Leader Init completed")
}


/*
	Checks Error
*/
func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
