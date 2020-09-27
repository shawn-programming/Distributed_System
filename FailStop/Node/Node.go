package node

import (
	"fmt"
	"strconv"
	"time"

	ms "../../FailStop/Membership"
)

/*
Node for a processor
*/
type Node struct {
	Id        ms.Id
	LocalTime int
	TimeOut   int
	MsList    ms.MsList
}

/*
node.Print()
*/
func (node Node) Print() {
	fmt.Println("TimeOut: ", node.TimeOut)
	node.MsList.Print()
}

/*
node.PrintLog()
RETURN: log
*/
func (node Node) PrintLog() string {
	log := "TimeOut: " + strconv.Itoa(node.TimeOut) + "/n"
	log += node.MsList.PrintLog()
	return log
}

/*
CreateNode(idNum, IPAddress string, localTime, timeOut int)
Node Constructor
RETURN: a Node for a processor
*/
func CreateNode(idNum, IPAddress string, localTime, timeOut int) Node {
	membership := ms.CreateMembership(idNum, IPAddress, 0, localTime)
	var membershipList ms.MsList
	membershipList = membershipList.Add(membership, localTime)

	ID := ms.Id{idNum, IPAddress}

	tempNode := Node{ID, localTime, timeOut, membershipList}

	return tempNode
}

/*
AddMember(member ms.Membership)

	Add a member to the node

RETURN:  node with the new member
*/
func (node Node) AddMember(member ms.Membership) Node {
	node.MsList = node.MsList.Add(member, node.LocalTime)
	return node
}

/*
IncrementLocalTime(inputList []ms.MsList)

	Increment local time of the node and update its data

RETURN: updated node
*/
func (node Node) IncrementLocalTime(inputList []ms.MsList) (Node, string) {
	node.LocalTime = node.LocalTime + 1
	var joinLog string
	var failLog string
	var removeLog string

	// wait for 1 sec
	time.Sleep(time.Second)

	// This is necessary for the case when there is no input
	node.MsList = node.MsList.UpdateMsList(ms.MsList{}, node.LocalTime, node.Id)

	var joinLogTotal string
	var removeLogTotal string

	for _, input := range inputList {
		// update newly join members and members' info
		node.MsList, joinLog = node.MsList.CheckMembers(input, node.LocalTime, node.TimeOut)
		joinLogTotal += joinLog
		node.MsList = node.MsList.UpdateMsList(input, node.LocalTime, node.Id)
	}

	// mark fails
	var removeList []ms.Id
	node.MsList, removeList, failLog = node.MsList.CheckFails(node.LocalTime, node.TimeOut)

	// remove timeout-ed members
	for _, removeit := range removeList {
		node.MsList, removeLog = node.MsList.Remove(removeit)
		removeLogTotal += removeLog
	}
	return node, joinLogTotal + failLog + removeLogTotal
}
