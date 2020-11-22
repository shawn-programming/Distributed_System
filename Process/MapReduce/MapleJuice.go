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
	"strings"

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
	csvfile.Close()

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

		filename := sdfs_intermediate_filename_prefix + ":" + strconv.Itoa(i) + ".csv"

		fmt.Println("Filename to write: " + filename)
		csvWriter(processNodePtr.LocalPath+filename, data_per_node[i])
		fmt.Print("File " + filename + " is written, uploading it to a sfds filelist")
		fs.Put(processNodePtr, filename, 1)
		fmt.Print("Uploading Done")
	}

	fmt.Println("Data split done")

	workerNodes := getNameNodes(processNodePtr, num_maples) // services of worker nodes
	SendUDPToWorkers(workerNodes, sdfs_intermediate_filename_prefix, sdfs_intermediate_filename_prefix)
}

func SendUDPToLeader(nodePtr *nd.Node, data []byte) {
	fmt.Println("SendUDPToLeader")

	leaderService := (*nodePtr.LeaderServicePtr)[0:len(*nodePtr.LeaderServicePtr)-4] + strconv.Itoa(nodePtr.DestPortNumMJ)
	fmt.Println("mjservice: ", leaderService)

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
func SendUDPToWorkers(workerNodes []string, filename string, sdfs_intermediate_filename_prefix string) {
	fmt.Println("SendUDPToWorkers start")

	for i, worker := range workerNodes {
		currFile := filename + ":" + strconv.Itoa(i) + ".csv"
		udpAddr, err := net.ResolveUDPAddr("udp4", worker)
		checkError(err)

		conn, err := net.DialUDP("udp", nil, udpAddr)
		checkError(err)

		_, err = conn.Write(pk.EncodePacket("Maple", pk.EncodeMapWorkerPacket(pk.MapWorker{currFile, sdfs_intermediate_filename_prefix})))

		var buf [512]byte
		_, err = conn.Read(buf[0:])
		checkError(err)
	}
	fmt.Println("SendUDPToWorkers Done")

}

func Wait(NodePtr *nd.Node, NumMaples int) {
	for {
		if *(NodePtr.MapleJuiceCounterPtr) == NumMaples {
			return
		}
	}
}

func MapleReceived(processNodePtr *nd.Node, sdfs_intermediate_filename_prefix string, fp func([]string) [][]string, input [][]string) {

	hashTable := make(map[string]int)
	var mapled_data [][][]string

	fmt.Println("start mapling")
	fmt.Println("Input size: " + strconv.Itoa(len(input)))
	for _, data := range input {
		//temp := fp(data)
		temp := CondorcetMapper1(data)

		// fmt.Println("Condocet mapper applied to temp")
		for _, datum := range temp {

			key := datum[0]
			//fmt.Println(key)
			if _, exists := hashTable[key]; !exists {
				//fmt.Println("Key does not exist")
				mapled_data = append(mapled_data, [][]string{})
				hashTable[key] = len(mapled_data) - 1
				//fmt.Println("allocating it at the idx " + strconv.Itoa(hashTable[key]))
			}

			//fmt.Println("hash done")
			location := hashTable[key]
			mapled_data[location] = append(mapled_data[location], datum)
			//fmt.Println("append hashed data done")
		}
	}

	fmt.Println("Data divisino Done")
	for key, location := range hashTable {
		filename := sdfs_intermediate_filename_prefix + ":" + key + ":" + processNodePtr.SelfIP + ".csv"

		csvWriter(processNodePtr.LocalPath+filename, mapled_data[location])
		fs.Put(processNodePtr, filename, 1)
	}

	fmt.Println("mapped file put done")
	fs.IncreaseMapleJuiceCounter(processNodePtr)
	fmt.Println("Increased MJ counter")
}

func MapleSort(processNodePtr *nd.Node, IntermediateFilename, SrcDirectory string) {

	// fmt.Println("Source Directory: " + SrcDirectory)
	// to_remove := fileList(SrcDirectory)
	// for _, f := range to_remove {
	// 	fs.Remove(processNodePtr, f)
	// 	fmt.Println("Removed " + f)
	// }

	fmt.Println("start maple sort")
	hashTable := make(map[string]int)
	fmt.Println("1")

	var mapled_data [][][]string

	// open all sdfs csv files starts with "IntermediateFilename" and store it as one total file.
	fmt.Println("2")

	for _, sdfsFile := range *(processNodePtr.DistributedFilesPtr) {
		fmt.Println("filename:", sdfsFile)

		if startsWith(sdfsFile, IntermediateFilename) {
			fmt.Println("start pulling")
			fs.Pull(processNodePtr, IntermediateFilename, 1)
			fmt.Println("Pulled:", sdfsFile)
		}
	}

	// Then save it as a csv file
	localFiles := fileList("./local_files")
	for _, local := range localFiles {
		fmt.Println("start for loop")
		fmt.Println("curr file:", local, " looking for:", IntermediateFilename)

		var temp [][]string
		if startsWith(local, IntermediateFilename) {
			temp = csvReader("./local_files/" + local)
			fmt.Println(len(temp))

			key := temp[0][0]

			if len(temp) > 0 {
				if _, exists := hashTable[key]; !exists {
					mapled_data = append(mapled_data, [][]string{})
					hashTable[key] = len(mapled_data) - 1
				}
				location := hashTable[key]
				mapled_data[location] = append(mapled_data[location], temp...)
			}
			//fs.Remove(processNodePtr, local)
		}

	}

	fmt.Println("done for loop")

	for key, location := range hashTable {
		filename := IntermediateFilename + ":" + key

		csvWriter(processNodePtr.LocalPath+filename, mapled_data[location])
		fs.Put(processNodePtr, filename, 1)
	}

}

func startsWith(s1, s2 string) bool {
	// if len(s1) > len(s2) && s1[:len(s2)] == s2 {
	// 	return true
	// } else {
	// 	return false
	// }

	return strings.HasPrefix(s1, s2)
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
