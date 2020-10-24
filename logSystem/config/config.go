package config

/*
Copied from https://courses.engr.illinois.edu/cs425/fa2020/assignments.html
The GO Version of
Recommended Solutions (from Student Submissions), From Fall 2019 semester: Go C++ Java Python
*/
import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

type configParam struct {
	IPAddress []string
	Port      []int
	PortHB    []int
	TimeOut   int
	K         int
	FailRate  int
}

// Opens the filename and attempts to deserialize it into a struct
func parseJSON(fileName string) (configParam, error) {
	file, err := ioutil.ReadFile(fileName)
	if err != nil {
		return configParam{}, err
	}

	//Necessities for go to be able to read JSON
	fileString := string(file)

	fileReader := strings.NewReader(fileString)

	decoder := json.NewDecoder(fileReader)

	var configParams configParam

	// Finally decode into json object
	err = decoder.Decode(&configParams)
	if err != nil {
		return configParam{}, err
	}

	return configParams, nil
}

// IPAddress gets list of all ip addresses from config.json
func IPAddress() ([]string, error) {
	configParams, err := parseJSON("./config.json")

	if err != nil {
		return make([]string, 0), err
	}

	// var configParams configParam
	// loadJSON("./config/config.json", &configParams)
	return configParams.IPAddress, nil
}

// Port gets the port number list from config.json
func Port() ([]int, error) {
	configParams, err := parseJSON("./config.json")

	if err != nil {
		return make([]int, 0), err
	}

	// var configParams configParam
	// loadJSON("./config/config.json", &configParams)

	return configParams.Port, nil
}

// Port gets the portHB number list from config.json
func PortHB() ([]int, error) {
	configParams, err := parseJSON("./config.json")

	if err != nil {
		return make([]int, 0), err
	}

	// var configParams configParam
	// loadJSON("./config/config.json", &configParams)

	return configParams.PortHB, nil
}

// TimeOut gets the timeout number from config.json
func TimeOut() (int, error) {
	configParams, err := parseJSON("./config.json")

	if err != nil {
		return -1, err
	}

	// var configParams configParam
	// loadJSON("./config/config.json", &configParams)

	return configParams.TimeOut, nil
}

// K gets the k number from config.json
func K() (int, error) {
	configParams, err := parseJSON("./config.json")

	if err != nil {
		return -1, err
	}

	// var configParams configParam
	// loadJSON("./config/config.json", &configParams)

	return configParams.K, nil
}

// FailRate gets the FailRate from config.json
func FailRate() (int, error) {
	configParams, err := parseJSON("./config.json")

	if err != nil {
		return -1, err
	}

	// var configParams configParam
	// loadJSON("./config/config.json", &configParams)

	return configParams.FailRate, nil
}
func loadJSON(fileName string, key interface{}) {
	inFile, err := os.Open(fileName)
	checkError(err)
	decoder := json.NewDecoder(inFile)
	err = decoder.Decode(key)
	checkError(err)
	inFile.Close()
}

func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
