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
	"time"

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

func SendUDPToLeader(nodePtr *nd.Node, data []byte, cmd string) {
	fmt.Println("SendUDPToLeader")

	leaderService := (*nodePtr.LeaderServicePtr)[0:len(*nodePtr.LeaderServicePtr)-4] + strconv.Itoa(nodePtr.DestPortNumMJ)
	fmt.Println("mjservice: ", leaderService)

	udpAddr, err := net.ResolveUDPAddr("udp4", leaderService)
	fs.CheckError(err)

	conn, err := net.DialUDP("udp", nil, udpAddr)
	fs.CheckError(err)

	_, err = conn.Write(pk.EncodePacket(cmd, data))

	var buf [512]byte
	_, err = conn.Read(buf[0:])
	fs.CheckError(err)
}

//send udp request to initiate maple sequence
func SendUDPToWorkers(workerNodes []string, filename string, sdfs_intermediate_filename_prefix string) {
	fmt.Println("SendUDPToWorkers start")

	for i, worker := range workerNodes {
		fmt.Println("sending udp to", worker)

		currFile := filename + ":" + strconv.Itoa(i) + ".csv"
		udpAddr, err := net.ResolveUDPAddr("udp4", worker)
		fs.CheckError(err)

		conn, err := net.DialUDP("udp", nil, udpAddr)
		fs.CheckError(err)

		_, err = conn.Write(pk.EncodePacket("Maple", pk.EncodeMapWorkerPacket(pk.MapWorker{currFile, sdfs_intermediate_filename_prefix})))

		var buf [512]byte
		_, err = conn.Read(buf[0:])
		fs.CheckError(err)
		fmt.Println("sent udp to", worker)
	}
	fmt.Println("SendUDPToWorkers Done")
}

func Wait(NodePtr *nd.Node, NumMaples int) {
	for {
		time.Sleep(time.Second)
		fmt.Println(*(NodePtr.MapleJuiceCounterPtr))
		if *(NodePtr.MapleJuiceCounterPtr) == NumMaples {
			*(NodePtr.MapleJuiceCounterPtr) = 0
			return
		}
	}
}

func one_string(input []string) string {
	result := ""

	for _, s := range input[:len(input)-1] {
		result = result + s + ":"
	}
	return result + input[len(input)-1]
}

func MapleReceived(processNodePtr *nd.Node, sdfs_intermediate_filename_prefix string, fp func([]string) [][][]string, input [][]string) {

	hashTable := make(map[string]int)
	var mapled_data [][][]string

	fmt.Println("start mapling")
	fmt.Println("Input size: " + strconv.Itoa(len(input)))
	for _, data := range input {
		//temp := fp(data)
		temp := CondorcetMapper1(data) //outputs [][][]string

		// fmt.Println("Condocet mapper applied  temp {to temp")
		for _, datum := range temp {

			key := one_string(datum[0]) //key - value pair
			value := datum[1]
			//fmt.Println(key)
			if _, exists := hashTable[key]; !exists {
				//fmt.Println("Key does not exist")
				mapled_data = append(mapled_data, [][]string{})
				hashTable[key] = len(mapled_data) - 1
				//fmt.Println("allocating it at the idx " + strconv.Itoa(hashTable[key]))
			}

			//fmt.Println("hash done")
			location := hashTable[key]
			mapled_data[location] = append(mapled_data[location], append([]string{key}, value...))
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

func MapleSort(processNodePtr *nd.Node, IntermediateFilename, SrcDirectory string) []string {

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

		if strings.HasPrefix(sdfsFile, IntermediateFilename) {
			fmt.Println("start pulling")
			fs.Pull(processNodePtr, sdfsFile, 1)
			fmt.Println("Pulled:", sdfsFile)
		}
	}

	// Then save it as a csv file
	localFiles := fileList("./local_files")
	for _, local := range localFiles {
		fmt.Println("start for loop")
		fmt.Println("curr file:", local, " looking for:", IntermediateFilename)

		var temp [][]string
		if strings.HasPrefix(local, IntermediateFilename) && strings.Contains(local, "172") {
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

	var fileList []string

	fmt.Println("done for loop")

	for key, location := range hashTable {
		filename := IntermediateFilename + ":" + key + "_mapled.csv"
		fmt.Println("Generating a file:", filename, " key:", key)
		csvWriter(processNodePtr.LocalPath+filename, mapled_data[location])
		fs.Put(processNodePtr, filename, 1)
		fileList = append(fileList, filename)
	}

	fmt.Println("maple sort done!")

	return fileList

}

func AllocateJuice(nodePtr *nd.Node, num_juice int, mapledList []string) map[string][]string {

	file_num := len(mapledList)
	if file_num < num_juice {
		num_juice = file_num
	}

	num_keys_per_node := (file_num - (file_num % num_juice)) / num_juice

	workerNodes := getNameNodes(nodePtr, num_juice)

	service_juice_pairs := make(map[string][]string)

	for i := 0; i < num_juice; i++ {
		start := num_keys_per_node * i
		end := num_keys_per_node * (i + 1)
		if i == num_juice-1 {
			end = len(mapledList)
		}

		allocatedJuice := mapledList[start:end]

		service_juice_pairs[workerNodes[i]] = allocatedJuice

		fmt.Println(service_juice_pairs, ":", allocatedJuice)
	}

	return service_juice_pairs
}

func Juice(processNodePtr *nd.Node, juice_exe string, num_juice int, sdfs_intermediate_filename_prefix, sdfs_src_directory string, delete_input bool) {

	service_juice_pairs := AllocateJuice(processNodePtr, num_juice, processNodePtr.MapledFiles)
	SendUDPJuiceToWorkers(service_juice_pairs, juice_exe, sdfs_intermediate_filename_prefix, sdfs_src_directory, delete_input)

}

func SendUDPJuiceToWorkers(service_juice_pairs map[string][]string,
	juice_exe string, sdfs_intermediate_filename_prefix string,
	sdfs_src_directory string,
	delete_input bool) {

	fmt.Println("SendJuiceUDPToWorkers start")

	for worker, filenames := range service_juice_pairs {

		udpAddr, err := net.ResolveUDPAddr("udp4", worker)
		fs.CheckError(err)

		conn, err := net.DialUDP("udp", nil, udpAddr)
		fs.CheckError(err)

		var data pk.MapJuiceWorker
		data.AllocatedFilenames = filenames
		data.JuiceExe = juice_exe
		data.IntermediateFilename = sdfs_intermediate_filename_prefix
		data.SrcDirectory = sdfs_src_directory
		data.DeleteOrNot = delete_input

		_, err = conn.Write(pk.EncodePacket("Juice", pk.EncodeMapJuiceWorkerPacket(data)))

		var buf [512]byte
		_, err = conn.Read(buf[0:])
		fs.CheckError(err)
	}
	fmt.Println("SendUDPJuiceToWorkers Done")
}

func JuiceReceived(nodePtr *nd.Node, fileList []string, juice_exe, sdfs_intermediate_filename_prefix string, delete_input bool) {
	var juiced_data [][]string

	for _, file := range fileList {
		fs.Pull(nodePtr, file, 1)
		data := csvReader(nodePtr.LocalPath + file)
		if juice_exe == "condorcet" {
			reduced_data := CondorcetReducer1(data)
			juiced_data = append(juiced_data, reduced_data)
		}
	}

	filename := sdfs_intermediate_filename_prefix + ":" + nodePtr.SelfIP + ".csv"

	csvWriter(filename, juiced_data)

	fmt.Println("Generated:", filename)

	fs.Put(nodePtr, filename, 1)

	fmt.Println("Pushed:", filename)

	fs.IncreaseMapleJuiceCounter(nodePtr)

	fmt.Println("Increased Juice Counter")
}

func JuiceSort(processNodePtr *nd.Node, IntermediateFilename, SrcDirectory string) {

	// fmt.Println("Source Directory: " + SrcDirectory)
	// to_remove := fileList(SrcDirectory)
	// for _, f := range to_remove {
	// 	fs.Remove(processNodePtr, f)
	// 	fmt.Println("Removed " + f)
	// }

	fmt.Println("start juice sort")

	var juiced_data [][]string

	// open all sdfs csv files starts with "IntermediateFilename" and store it as one total file.

	for _, sdfsFile := range *(processNodePtr.DistributedFilesPtr) {
		fmt.Println("filename:", sdfsFile)

		if strings.HasPrefix(sdfsFile, IntermediateFilename) {
			fmt.Println("start pulling")
			fs.Pull(processNodePtr, sdfsFile, 1)
			fmt.Println("Pulled:", sdfsFile)
		}
	}

	// Then save it as a csv file
	localFiles := fileList("./local_files")
	for _, local := range localFiles {
		fmt.Println("start for loop")
		fmt.Println("curr file:", local, " looking for:", IntermediateFilename)

		if strings.HasPrefix(local, IntermediateFilename) && strings.Contains(local, "172") {
			temp := csvReader("./local_files/" + local)
			fmt.Println(len(temp))

			if len(temp) > 0 {
				juiced_data = append(juiced_data, temp...)
			}
			//fs.Remove(processNodePtr, local)
		}

	}

	fmt.Println("done for loop")

	filename := IntermediateFilename + "_juiced.csv"
	fmt.Println("Generating a file:", filename)
	csvWriter(processNodePtr.LocalPath+filename, juiced_data)
	fs.Put(processNodePtr, filename, 1)

	fmt.Println("juice sort done!")

}

func CondorcetMapper1(input []string) [][][]string {
	mapledData := [][][]string{}

	m := len(input)
	for i := 0; i < m-1; i++ {
		for j := i + 1; j < m; j++ {
			if input[i] < input[j] {
				temp := [][]string{[]string{input[i], input[j]}, []string{"1"}}
				mapledData = append(mapledData, temp)
			} else {
				temp := [][]string{[]string{input[j], input[i]}, []string{"0"}}
				mapledData = append(mapledData, temp)
			}
		}
	}
	return mapledData
}

func CondorcetReducer1(input [][]string) []string {
	Acount := 0
	Bcount := 0

	var keyA string
	var keyB string
	for _, line := range input {
		keys := strings.Split(line[0], ":")
		entry := line[1]

		keyA = keys[0]
		keyB = keys[1]

		if entry == "1" { // A won
			Acount++
		} else { // B won
			Bcount++
		}
	}

	if Acount > Bcount {
		return []string{keyA, keyB}
	}
	return []string{keyB, keyA}
}
