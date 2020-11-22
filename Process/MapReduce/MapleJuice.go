package MapleJuice

import (
	//"bufio"
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"

	fs "../FileSys"
	nd "../Node"
	pk "../Packet"
)

func csvWriter(filePath string, data [][]string) {
	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}

	wr := csv.NewWriter(bufio.NewWriter(file))

	for _, datum := range data {
		wr.Write(datum)
	}

	wr.Flush()
}

// code copied from https://medium.com/@ankurraina/reading-a-simple-csv-in-go-36d7a269cecd
func csvReader(filePath string) [][]string {
	// Open the file
	fmt.Print("csvReader: reading this file ", filePath)
	csvfile, err := os.Open(filePath)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	// Parse the file
	r := csv.NewReader(csvfile)
	//r := csv.NewReader(bufio.NewReader(csvfile))

	data := [][]string{}
	// Iterate through the records
	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		data = append(data, record)
	}

	fmt.Println("csv Reader done")
	return data
}

// return the list of files
func fileList(directory string) []string {
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		log.Fatal(err)
	}
	var filename []string
	for _, file := range files {
		filename = append(filename, file.Name())
	}

	return filename
}

// get services of worker nodes
func getNameNodes(processNodePtr *nd.Node, num_maples int) []string {
	var workerNodes []string
	count := 0
	for _, mem := range processNodePtr.MsList.List {
		if count == num_maples {
			return workerNodes
		}

		if mem.ID.IPAddress != processNodePtr.SelfIP {
			workerNodes = append(workerNodes, mem.ID.IPAddress+":"+strconv.Itoa(processNodePtr.DestPortNumMJ))
			count++
		}
	}

	return workerNodes
}

func Maple(processNodePtr *nd.Node, maple_exe string, num_maples int, sdfs_intermediate_filename_prefix, sdfs_src_directory string) {
	files := fileList(sdfs_src_directory)

	var input_data [][]string

	for _, file := range files {
		fmt.Println("File: " + file)
		input_data = append(input_data, csvReader(sdfs_src_directory+file)...)
	}

	fmt.Println("Finished opening files, number of files opented: " + strconv.Itoa(len(input_data)))

	input_num := len(input_data)
	input_per_node := (input_num - input_num%num_maples) / num_maples

	fmt.Println("Input per node: " + strconv.Itoa(input_per_node))

	var data_per_node [][][]string
	for i := 0; i < num_maples; i++ {
		start := input_per_node * i
		end := input_per_node * (i + 1)
		if i == num_maples-1 {
			end = input_num
		}
		data_per_node = append(data_per_node, input_data[start:end])

		filename := sdfs_intermediate_filename_prefix + ":" + strconv.Itoa(i)

		fmt.Println("Filename to write: " + filename)
		csvWriter(processNodePtr.LocalPath+filename, data_per_node[i])
		fmt.Print("File " + filename + " is written, uploading it to a sfds filelist")
		fs.Put(processNodePtr, filename, 1)
		fmt.Print("Uploading Done")
	}

	fmt.Println("Data split done")

	workerNodes := getNameNodes(processNodePtr, num_maples) // services of worker nodes
	SendUDPToWorkers(workerNodes, sdfs_intermediate_filename_prefix, sdfs_src_directory)
}

func SendUDPToLeader(nodePtr *nd.Node, data []byte) {

	leaderService := (*nodePtr.LeaderServicePtr)[0:len(*nodePtr.LeaderServicePtr)-4] + strconv.Itoa(nodePtr.DestPortNumMJ)

	udpAddr, err := net.ResolveUDPAddr("udp4", leaderService)
	checkError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	checkError(err)

	_, err = conn.Write(pk.EncodePacket("StartMaple", data))

	var buf [512]byte
	_, err = conn.Read(buf[0:])
	checkError(err)
}

//send udp request to initiate maple sequence
func SendUDPToWorkers(workerNodes []string, filename string, src_directory string) {
	fmt.Println("SendUDPToWorkers start")

	for i, worker := range workerNodes {
		currFile := filename + ":" + strconv.Itoa(i)
		udpAddr, err := net.ResolveUDPAddr("udp4", worker)
		checkError(err)

		conn, err := net.DialUDP("udp", nil, udpAddr)
		checkError(err)

		_, err = conn.Write(pk.EncodePacket("Maple", pk.EncodeMapWorkerPacket(pk.MapWorker{currFile, src_directory})))

		var buf [512]byte
		_, err = conn.Read(buf[0:])
		checkError(err)
	}
	fmt.Println("SendUDPToWorkers Done")

}

func Wait(NodePtr *nd.Node, NumMaples int) {
	for {
		if *(NodePtr.MapleJuiceCounterPtr) == NumMaples-1 {
			return
		}
	}
}

func MapleReceived(processNodePtr *nd.Node, sdfs_intermediate_filename_prefix string, fp func([]string) [][]string, input [][]string) {

	var hashTable map[string]int
	var mapled_data [][][]string

	for _, data := range input {
		//temp := fp(data)
		temp := CondorcetMapper1(data)

		for _, datum := range temp {
			key := datum[0]
			if _, exists := hashTable[key]; !exists {
				mapled_data = append(mapled_data, [][]string{})
				hashTable[key] = len(mapled_data)
			}

			location := hashTable[key]
			mapled_data[location] = append(mapled_data[location], datum)
		}
	}

	for key, location := range hashTable {
		filename := sdfs_intermediate_filename_prefix + ":" + key + ":" + processNodePtr.SelfIP

		csvWriter(processNodePtr.LocalPath+filename, mapled_data[location])
		fs.Put(processNodePtr, filename, 1)
	}

	fs.IncreaseMapleJuiceCounter(processNodePtr)
}

func MapleSort(processNodePtr *nd.Node, IntermediateFilename, SrcDirectory string) {

	to_remove := fileList(SrcDirectory)
	for _, f := range to_remove {
		fs.Remove(processNodePtr, f)
	}

	var hashTable map[string]int
	var mapled_data [][][]string

	// open all sdfs csv files starts with "IntermediateFilename" and store it as one total file.

	for _, sdfsFile := range *(processNodePtr.DistributedFilesPtr) {
		if startsWith(sdfsFile, IntermediateFilename) {
			fs.Pull(processNodePtr, IntermediateFilename, 1)
		}
	}

	// Then save it as a csv file
	localFiles := fileList("./local_files")
	for _, local := range localFiles {
		var temp [][]string
		if startsWith(local, IntermediateFilename) {
			temp = csvReader("./local_files/" + local)
		} else {
			continue
		}

		key := temp[0][0]

		if len(temp) > 0 {
			if _, exists := hashTable[key]; !exists {
				mapled_data = append(mapled_data, [][]string{})
				hashTable[key] = len(mapled_data)
			}
			location := hashTable[key]
			mapled_data[location] = append(mapled_data[location], temp...)
		}

		fs.Remove(processNodePtr, local)
	}

	for key, location := range hashTable {
		filename := IntermediateFilename + ":" + key

		csvWriter(processNodePtr.LocalPath+filename, mapled_data[location])
		fs.Put(processNodePtr, filename, 1)
	}
}

func startsWith(s1, s2 string) bool {
	if len(s1) > len(s2) && s1[:len(s2)] == s2 {
		return true
	} else {
		return false
	}
}
func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}

func CondorcetMapper1(input []string) [][]string {
	mapledData := [][]string{}

	m := len(input)
	for i := 0; i < m-1; i++ {
		for j := i + 1; j < m; j++ {
			if input[i] < input[j] {
				temp := []string{input[i], input[j], "1"}
				mapledData = append(mapledData, temp)
			} else {
				temp := []string{input[j], input[i], "0"}
				mapledData = append(mapledData, temp)
			}
		}
	}
	return mapledData
}