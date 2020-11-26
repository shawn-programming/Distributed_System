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

	//"os"
	"strconv"
	"strings"

	fs "../FileSys"
	nd "../Node"
	pk "../Packet"
)

/*

Maple(<maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>):
	when a working node processes a file:
		outputs -> maple_<sdfs_intermediate_filename_prefix>_selfIP:key.csv

	when a leader node sum up all of the data:
		outputs -> maple_<maple_exe>:key.csv

Juice( <exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename>
delete_input={0,1}):
	when a working node processes a file:
		outputs -> juice_<sdfs_intermediate_filename_prefix>_IP.csv

	when a leader node sum up all of the data:
		outputs -> <sdfs_dest_filename>.csv

*/

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

func ReadFromCsv(filePath string) ([][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(file)
	stringValues, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return stringValues, nil
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
	SendUDPToWorkers(processNodePtr, workerNodes, sdfs_intermediate_filename_prefix, sdfs_intermediate_filename_prefix, maple_exe)
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
func SendUDPToWorkers(nodePtr *nd.Node, workerNodes []string, filename string, sdfs_intermediate_filename_prefix string, mapleExe string) {
	fmt.Println("SendUDPToWorkers start")

	for i, worker := range workerNodes {
		fmt.Println("sending udp to", worker)

		currFile := filename + ":" + strconv.Itoa(i) + ".csv"
		udpAddr, err := net.ResolveUDPAddr("udp4", worker)
		fs.CheckError(err)

		conn, err := net.DialUDP("udp", nil, udpAddr)
		fs.CheckError(err)

		packet := pk.EncodePacket("Maple", pk.EncodeMapWorkerPacket(pk.MapWorker{currFile, sdfs_intermediate_filename_prefix, mapleExe}))
		_, err = conn.Write(packet)

		var buf [512]byte
		_, err = conn.Read(buf[0:])

		IPAddress := worker[:len(worker)-5]

		newinput := (*nodePtr.MapleJuiceProcessPtr)[IPAddress]
		newinput.Status = "busy"
		newinput.Query = packet
		(*nodePtr.MapleJuiceProcessPtr)[worker] = newinput

		fs.CheckError(err)
		fmt.Println("sent udp to", worker)
	}
	fmt.Println("SendUDPToWorkers Done")
}

func SendUDPJuiceToWorkers(nodePtr *nd.Node, service_juice_pairs map[string][]string,

	juice_exe string, sdfs_intermediate_filename_prefix string,
	sdfs_src_directory string,
	delete_input bool) {

	fmt.Println("SendJuiceUDPToWorkers start")

	for worker, filenames := range service_juice_pairs {
		fmt.Println("sending: ", worker)

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

		packet := pk.EncodePacket("Juice", pk.EncodeMapJuiceWorkerPacket(data))
		_, err = conn.Write(packet)

		var buf [512]byte
		_, err = conn.Read(buf[0:])

		IPAddress := worker[:len(worker)-5]

		newinput := (*nodePtr.MapleJuiceProcessPtr)[IPAddress]
		newinput.Status = "busy"
		newinput.Query = packet
		(*nodePtr.MapleJuiceProcessPtr)[IPAddress] = newinput

		fs.CheckError(err)
		fmt.Println("sent udp to", worker)

		fs.CheckError(err)
	}
	fmt.Println("SendUDPJuiceToWorkers Done")
}

func getFreeProcess(nodePtr *nd.Node) string {
	for IP, info := range *(nodePtr.MapleJuiceProcessPtr) {
		if info.Status == "free" && IP != nodePtr.SelfIP {
			return IP
		}
	}
	fmt.Println("No process available. Fata Error")
	return ""
}

func freeAll(nodePtr *nd.Node) {
	for IP, info := range *nodePtr.MapleJuiceProcessPtr {
		if info.Status == "busy" {

			newinput := (*nodePtr.MapleJuiceProcessPtr)[IP]
			newinput.Status = "free"
			(*nodePtr.MapleJuiceProcessPtr)[IP] = newinput
		}
	}
}

func checkProcesses(nodePtr *nd.Node) {
	for _, info := range *nodePtr.MapleJuiceProcessPtr {
		if info.Status == "failed" {
			free := getFreeProcess(nodePtr)
			freeService := free + ":" + strconv.Itoa(nodePtr.DestPortNumMJ)
			query := info.Query
			fmt.Println("Reassigning the task to", freeService)

			udpAddr, err := net.ResolveUDPAddr("udp4", freeService)
			fs.CheckError(err)

			conn, err := net.DialUDP("udp", nil, udpAddr)
			fs.CheckError(err)

			_, err = conn.Write(query)

			var buf [512]byte
			_, err = conn.Read(buf[0:])

			newinput := (*nodePtr.MapleJuiceProcessPtr)[free]
			newinput.Status = "busy"
			newinput.Query = query
			(*nodePtr.MapleJuiceProcessPtr)[free] = newinput
		}
	}
}

func Wait(nodePtr *nd.Node, NumMaples int) {
	for {
		checkProcesses(nodePtr)
		if *(nodePtr.MapleJuiceCounterPtr) == NumMaples {
			*(nodePtr.MapleJuiceCounterPtr) = 0
			freeAll(nodePtr)
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
		temp := fp(data) //outputs [][][]string

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

	fileMadeList := []string{}
	fmt.Println("Data divisino Done")
	for key, location := range hashTable {
		filename := "maple_" + sdfs_intermediate_filename_prefix + "_" + processNodePtr.SelfIP + ":" + key + ".csv"
		fileMadeList = append(fileMadeList, filename)
		csvWriter(processNodePtr.LocalPath+filename, mapled_data[location])
		fmt.Println("Before PUT")
		fs.Put(processNodePtr, filename, 1)
		fmt.Println("After PUT")

	}

	fmt.Println("mapped file put done")
	fs.IncreaseMapleJuiceCounter(processNodePtr, fileMadeList)
	fmt.Println("Increased MJ counter")
}

func MapleSort(processNodePtr *nd.Node, maple_exe, IntermediateFilename, SrcDirectory string) []string {

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

	// distributedFiles := fs.GetFileList(processNodePtr)

	// for sdfsFile, _ := range distributedFiles {
	// 	fmt.Println("filename:", sdfsFile)

	// 	if strings.HasPrefix(sdfsFile, "maple_"+IntermediateFilename) {
	for _, file := range *(processNodePtr.MapleJuiceFileListPtr) {
		fmt.Println("start pulling", file)
		fs.Pull(processNodePtr, file, 1)

		for {
			data, _ := ReadFromCsv(processNodePtr.LocalPath + file)
			if len(data) > 0 {
				break
			}
			//fmt.Println("juice:")
		}
		fmt.Println("Pulled:", file)
	}

	// Then save it as a csv file
	localFiles := fileList("./local_files")
	for _, local := range localFiles {
		fmt.Println("start for loop")
		fmt.Println("curr file:", local, " looking for:", IntermediateFilename)

		var temp [][]string
		if strings.HasPrefix(local, "maple_"+IntermediateFilename) && strings.Contains(local, "172") {
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
		filename := "mapled_" + maple_exe + ":" + key + ".csv"
		fmt.Println("Generating a file:", filename, " key:", key)
		csvWriter(processNodePtr.LocalPath+filename, mapled_data[location])
		fs.Put(processNodePtr, filename, 1)
		fileList = append(fileList, filename)
	}

	fmt.Println("maple sort done!")

	return fileList
}

func AllocateJuice(nodePtr *nd.Node, num_juice int, mapledList []string) map[string][]string {

	print("mapledlist:", mapledList)
	file_num := len(mapledList)
	println(mapledList)
	if file_num < num_juice {
		num_juice = file_num
	}
	fmt.Println("File num:", file_num, "num_juice:", num_juice)

	num_keys_per_node := (file_num - (file_num % num_juice)) / num_juice

	fmt.Println("num_keys_per_node", num_keys_per_node)

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

	for key, file := range service_juice_pairs {
		fmt.Println("vm:", key)
		fmt.Println("Assigned file:", file)
	}

	return service_juice_pairs
}

func Juice(processNodePtr *nd.Node, juice_exe string, num_juice int, sdfs_intermediate_filename_prefix, sdfs_src_directory string, delete_input bool) {
	fmt.Println("Juice")

	service_juice_pairs := AllocateJuice(processNodePtr, num_juice, (*processNodePtr.MapledFilesPtr))
	SendUDPJuiceToWorkers(processNodePtr, service_juice_pairs, juice_exe, sdfs_intermediate_filename_prefix, sdfs_src_directory, delete_input)

	fmt.Println("Juice Done")

}

func JuiceReceived(nodePtr *nd.Node, fileList []string, fp func([][]string) [][]string, sdfs_intermediate_filename_prefix string, delete_input bool) {
	var juiced_data [][]string

	for _, file := range fileList {
		fmt.Println("reading file...:", file)

		fs.Pull(nodePtr, file, 1)
		var data [][]string

		for {
			data, _ = ReadFromCsv(nodePtr.LocalPath + file)
			if len(data) > 0 {
				break
			}
			//fmt.Println("juice:")
		}
		reduced_data := fp(data)

		juiced_data = append(juiced_data, reduced_data...)
	}

	filename := "juice_" + sdfs_intermediate_filename_prefix + "_" + nodePtr.SelfIP + ".csv"

	csvWriter(nodePtr.LocalPath+filename, juiced_data)

	fmt.Println("Generated:", filename)

	fs.Put(nodePtr, filename, 1)

	fmt.Println("Pushed:", filename)

	fs.IncreaseMapleJuiceCounter(nodePtr, []string{filename})

	fmt.Println("Increased Juice Counter")
}

func JuiceSort(processNodePtr *nd.Node, juice_exe, IntermediateFilename, sdfs_dest_filename string) {

	// fmt.Println("Source Directory: " + SrcDirectory)
	// to_remove := fileList(SrcDirectory)
	// for _, f := range to_remove {
	// 	fs.Remove(processNodePtr, f)
	// 	fmt.Println("Removed " + f)
	// }

	fmt.Println("start juice sort")

	var juiced_data [][]string

	// open all sdfs csv files starts with "IntermediateFilename" and store it as one total file.

	// distributedFiles := fs.GetFileList(processNodePtr)
	for _, file := range *(processNodePtr.MapleJuiceFileListPtr) {
		fmt.Println("start pulling", file)
		fs.Pull(processNodePtr, file, 1)

		for {
			data, _ := ReadFromCsv(processNodePtr.LocalPath + file)
			if len(data) > 0 {
				break
			}
			//fmt.Println("juice:")
		}
		fmt.Println("Pulled:", file)
	}

	// Then save it as a csv file
	localFiles := fileList("./local_files")
	for _, local := range localFiles {
		fmt.Println("start for loop")
		fmt.Println("curr file:", local, " looking for:", IntermediateFilename)

		if strings.HasPrefix(local, "juice_"+IntermediateFilename) && strings.Contains(local, "172") {
			temp := csvReader("./local_files/" + local)
			fmt.Println(len(temp))

			if len(temp) > 0 {
				juiced_data = append(juiced_data, temp...)
			}
			//fs.Remove(processNodePtr, local)
		}

	}

	fmt.Println("done for loop")

	_ = os.MkdirAll(sdfs_dest_filename, 0755)

	filename := sdfs_dest_filename + ".csv"
	fmt.Println("Generating a file:", filename)

	csvWriter(sdfs_dest_filename+"/"+filename, juiced_data)
	csvWriter(processNodePtr.LocalPath+filename, juiced_data)
	fs.Put(processNodePtr, filename, 1)

	fmt.Println("juice sort done!")

}
