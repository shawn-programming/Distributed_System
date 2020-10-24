package Packet

import (
	"encoding/json"
	"fmt"
	"os"

	ms "../Membership"
)

type PacketType struct {
	Ptype        string
	EncodePacket []byte
}

/*
EncodeJSON(message Packet) []byte
	Encodes message Packet into byte slice
*/
func EncodePacket(ptype string, packet []byte) []byte {
	encodedMessage, err := json.Marshal(PacketType{ptype, packet})
	checkError(err)
	return encodedMessage
}

/*
DecodeJSON(encodedMessage []byte) Packet
	Decodes byte slice into message Packet
*/
func DecodePacket(encodedMessage []byte) PacketType {
	var decodedMessage PacketType
	err := json.Unmarshal(encodedMessage, &decodedMessage)
	checkError(err)
	return decodedMessage
}

/*
struct for Packet used in UDP communication
*/
type HBpacket struct {
	Input            ms.MsList //input msList of the sender process
	IsInitialization bool
}

/*
EncodeJSON(message Packet) []byte
	Encodes message Packet into byte slice
*/
func EncodeHB(message HBpacket) []byte {
	encodedMessage, err := json.Marshal(message)
	checkError(err)
	return encodedMessage
}

/*
DecodeJSON(encodedMessage []byte) Packet
	Decodes byte slice into message Packet
*/
func DecodeHB(encodedMessage PacketType) HBpacket {
	var decodedMessage HBpacket
	err := json.Unmarshal(encodedMessage.EncodePacket, &decodedMessage)
	checkError(err)
	return decodedMessage
}

/*
struct for Packet used for finding the list of ids for file replication
*/
type IdListpacket struct {
	N          int
	OriginalID ms.Id
	List       []ms.Id //input msList of the sender process

	Filename string
}

/*
EncodeIdList(message Packet) []byte
	Encodes message Packet into byte slice
*/
func EncodeIdList(message IdListpacket) []byte {
	encodedMessage, err := json.Marshal(message)
	checkError(err)
	return encodedMessage
}

/*
DecodeIdList(encodedMessage []byte) Packet
	Decodes byte slice into message Packet
*/
func DecodeIdList(encodedMessage PacketType) IdListpacket {
	var decodedMessage IdListpacket
	err := json.Unmarshal(encodedMessage.EncodePacket, &decodedMessage)
	checkError(err)
	return decodedMessage
}

/*
struct for Packet used for sending the list of files to  the list of ids for file replication
*/
type Putpacket struct {
	Id       ms.Id
	Filename string
}

/*
EncodeIdList(message Packet) []byte
	Encodes message Packet into byte slice
*/
func EncodePut(message Putpacket) []byte {
	encodedMessage, err := json.Marshal(message)
	checkError(err)
	return encodedMessage
}

/*
DecodeIdList(encodedMessage []byte) Packet
	Decodes byte slice into message Packet
*/
func DecodePut(encodedMessage PacketType) Putpacket {
	var decodedMessage Putpacket
	err := json.Unmarshal(encodedMessage.EncodePacket, &decodedMessage)
	checkError(err)
	return decodedMessage
}

/*
struct for Packet used for sending the list of files to  the list of ids for file replication
*/
type FileListpacket struct {
	FileList map[string][]ms.Id
}

/*
EncodeIdList(message Packet) []byte
	Encodes message Packet into byte slice
*/
func EncodeFileList(message FileListpacket) []byte {
	encodedMessage, err := json.Marshal(message)
	checkError(err)
	return encodedMessage
}

/*
DecodeIdList(encodedMessage []byte) Packet
	Decodes byte slice into message Packet
*/
func DecodeFileList(encodedMessage PacketType) FileListpacket {
	var decodedMessage FileListpacket
	err := json.Unmarshal(encodedMessage.EncodePacket, &decodedMessage)
	checkError(err)
	return decodedMessage
}

type TCPcmd struct {
	Cmd      string
	Filename string
	IsPull   bool
}

func EncodeTCPcmd(message TCPcmd) []byte {
	encodedMessage, err := json.Marshal(message)
	checkError(err)
	return encodedMessage
}

func DecodeTCPcmd(encodedMessage PacketType) TCPcmd {
	var decodedMessage TCPcmd
	err := json.Unmarshal(encodedMessage.EncodePacket, &decodedMessage)
	checkError(err)
	return decodedMessage
}

type TCPsend struct {
	ToList   []ms.Id
	Filename string
	IsPull   bool
}

func EncodeTCPsend(message TCPsend) []byte {
	encodedMessage, err := json.Marshal(message)
	checkError(err)
	return encodedMessage
}

func DecodeTCPsend(encodedMessage PacketType) TCPsend {
	var decodedMessage TCPsend
	err := json.Unmarshal(encodedMessage.EncodePacket, &decodedMessage)
	checkError(err)
	return decodedMessage
}

type RingData struct {
	Elected   bool
	YourIndex int
	Ring      []string
	Initiator string
	NewLeader string
}

func EncodeRingData(message RingData) []byte {
	encodedMessage, err := json.Marshal(message)
	checkError(err)
	return encodedMessage
}

func DecodeRingData(encodedMessage PacketType) RingData {
	var decodedMessage RingData
	err := json.Unmarshal(encodedMessage.EncodePacket, &decodedMessage)
	checkError(err)
	return decodedMessage
}

type FilesPacket struct {
	Id       ms.Id
	FileName []string
}

func EncodeFilesPacket(message FilesPacket) []byte {
	encodedMessage, err := json.Marshal(message)
	checkError(err)
	return encodedMessage
}

func DecodeFilesPacket(encodedMessage PacketType) FilesPacket {
	var decodedMessage FilesPacket
	err := json.Unmarshal(encodedMessage.EncodePacket, &decodedMessage)
	checkError(err)
	return decodedMessage
}

/*
CheckError(err error)
	Terminate system with message, if Error occurs
*/
func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
