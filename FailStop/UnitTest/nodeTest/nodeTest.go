package nodetest

import (
	"fmt"

	ms "../../Membership"
	nd "../../node"
)

/*
Testing Constructor of Node
*/
func ConstructorTest() {
	newNode := nd.CreateNode("10", "abcd", 100, 10)
	newNode.Print()
}

/*
Testing AddMemberTest() of Node
*/
func AddMemberTest() {
	newNode := nd.CreateNode("15", "abcd", 100, 10)
	newNode.Print()

	fmt.Println("After Adding (20, xyz, 10, 1),")
	temp1 := ms.CreateMembership("20", "xyz", 10, 1)
	newNode = newNode.AddMember(temp1)
	newNode.Print()

	fmt.Println("After Adding (11, lol, -1, -1),")
	temp2 := ms.CreateMembership("11", "lol", -1, -1)
	newNode = newNode.AddMember(temp2)
	newNode.Print()
}

/*
Testing IncrementLocalTime()
*/
func IncrementLocalTimeTest() {
	newNode := nd.CreateNode("5", "11:22:33:44", 0, 5)
	temp1 := ms.CreateMembership("6", "22.11.33.44", 10, 5)
	temp2 := ms.CreateMembership("7", "44.55.11.22", 2, 5)
	newNode = newNode.AddMember(temp1)
	newNode = newNode.AddMember(temp2)

	secondNode := nd.CreateNode("8", "11:22:33:44", 0, 5)
	temp3 := ms.CreateMembership("6", "22.11.33.44", 10, 5)
	temp4 := ms.CreateMembership("7", "44.55.11.22", 2, 5)
	secondNode = secondNode.AddMember(temp3)
	secondNode = secondNode.AddMember(temp4)

	fmt.Println("First Node")
	newNode.Print()
	fmt.Println("Second Node")
	secondNode.Print()

	var failList []ms.Id
	inputList := []ms.MsList{secondNode.MsList}
	failList, newNode = newNode.IncrementLocalTime(inputList)

	fmt.Println("After 1 local time passed")
	newNode.Print()
	fmt.Println(failList)
}
