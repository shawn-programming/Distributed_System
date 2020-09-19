package node

import (
	"fmt"

	ms "../Membership"
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
	// node.id.Print()
	fmt.Println("TimeOut: ", node.TimeOut)
	node.MsList.Print()
}

/*
CreateNode(idNum, IPAddress string, localTime, timeOut int)
Node Constructor
	return a Node for a processor
*/
func CreateNode(idNum, IPAddress string, localTime, timeOut int) Node {
	membership := ms.CreateMembership(idNum, IPAddress, localTime, timeOut)
	var membershipList ms.MsList
	membershipList = membershipList.Add(membership, localTime)

	ID := ms.Id{idNum, IPAddress}

	tempNode := Node{ID, localTime, timeOut, membershipList}

	return tempNode
}

/*
(node Node) AddMember(member ms.Membership)
	RETURN: Node

	add a member to the node's msList
*/
func (node Node) AddMember(member ms.Membership) Node {
	node.MsList = node.MsList.Add(member, node.LocalTime)
	return node
}

// /*
// (node Node) AtaCheckMember(toCompare ms.MsList)
// 	* ALL-TO-ALL Function
// 	RETURN: Node

// 	compare node's msList with the input.
// 	If the input misses any of member the node has, remove it from the node
// */
// func (node Node) AtaCheckMember(toCompare ms.MsList) Node {
// 	node.msList = node.msList.CheckMember(toCompare)
// 	return node
// }

/*
	(node Node) IncrementLocalTime(inputList []ms.MsList)
	RETURN: node

	1. Increment the local time

	2. Update the MsList
		a) first check curr node with the input node
			-> if there is a failed in an input node, remove from current list as well

		b) update curr nodes with input node


		c) remove failed nodes
*/
func (node Node) IncrementLocalTime(inputList []ms.MsList) ([]ms.Id, Node) {
	node.LocalTime = node.LocalTime + 1

	var failList []ms.Id

	for _, input := range inputList {
		// node.msList = node.msList.CheckMember(input)

		node.MsList = node.MsList.UpdateMsList(input, node.LocalTime)
	}
	failList = node.MsList.CheckFails(node.LocalTime, node.TimeOut)

	for _, failed := range failList {
		node.MsList = node.MsList.Remove(failed)
	}

	return failList, node
}
