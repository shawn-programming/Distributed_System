package nodeTest

import (
	"fmt"

	ms "../../Membership"
	nd "../../node"
)

func main() {
	//ConstructorTest()
	//AddMemberTest()
	IncrementLocalTimeTest()
}

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
	var log string
	fmt.Println("====================Init======================== starting phase")
	// # initialize a node with 3 members
	baseNode := nd.CreateNode("1", "11:22:33:44", 0, 3)
	temp1 := ms.CreateMembership("1", "11:22:33:44", 0, 3)
	temp2 := ms.CreateMembership("2", "22.11.33.44", 0, 3)
	temp3 := ms.CreateMembership("3", "44.55.11.22", 0, 3)
	baseNode = baseNode.AddMember(temp2)
	baseNode = baseNode.AddMember(temp3)
	baseNode.MsList.Print()

	fmt.Println("===========After 1 local time passed============ do nothing")
	// first round, do nothing
	baseNode, log = baseNode.IncrementLocalTime([]ms.MsList{})
	baseNode.MsList.Print()

	fmt.Println("===========After 2 local time passed============ update member1")
	// second round, update "member 1"
	var secondRound ms.MsList
	temp1 = ms.CreateMembership("1", "11:22:33:44", 1, 3)
	secondRound = secondRound.Add(temp1, 2)
	secondRound = secondRound.Add(temp2, 2)
	secondRound = secondRound.Add(temp3, 2)
	baseNode, log = baseNode.IncrementLocalTime([]ms.MsList{secondRound})
	baseNode.MsList.Print()

	fmt.Println("===========After 3 local time passed============ update member2")
	// third round, update member 2"
	var thirdRound ms.MsList
	temp2 = ms.CreateMembership("2", "22.11.33.44", 1, 3)
	thirdRound = thirdRound.Add(temp1, 2)
	thirdRound = thirdRound.Add(temp2, 2)
	thirdRound = thirdRound.Add(temp3, 2)
	baseNode, log = baseNode.IncrementLocalTime([]ms.MsList{thirdRound})
	baseNode.MsList.Print()

	fmt.Println("===========After 4 local time passed============ should make member 3 fail")
	// fourth round, member 3 failed"
	baseNode, log = baseNode.IncrementLocalTime([]ms.MsList{})
	baseNode.MsList.Print()

	fmt.Println("===========After 5 local time passed============ new member 4")
	// fifth round new member, member 4
	var fifthRound ms.MsList
	temp4 := ms.CreateMembership("4", "44.55.66.77", 0, 3)
	fifthRound = fifthRound.Add(temp1, 2)
	fifthRound = fifthRound.Add(temp2, 2)
	fifthRound = fifthRound.Add(temp3, 2)
	fifthRound = fifthRound.Add(temp4, 2)
	baseNode, log = baseNode.IncrementLocalTime([]ms.MsList{fifthRound})
	baseNode.MsList.Print()

	temp5 := ms.CreateMembership("5", "88.55.66.11", 0, 3)
	// sixth round new member, member 4
	var sixthRound ms.MsList
	temp5.Failed = true
	sixthRound = sixthRound.Add(temp1, 2)
	sixthRound = sixthRound.Add(temp2, 2)
	sixthRound = sixthRound.Add(temp3, 2)
	sixthRound = sixthRound.Add(temp4, 2)
	sixthRound = sixthRound.Add(temp5, 2)

	fmt.Println("===========After 6 local time passed============ should do nothing")
	baseNode, log = baseNode.IncrementLocalTime([]ms.MsList{sixthRound})
	baseNode.MsList.Print()

	fmt.Println("===========After 7 local time passed============ member 3 should be removed")
	// last round, member 3 should be removed"
	baseNode, log = baseNode.IncrementLocalTime([]ms.MsList{})
	baseNode.MsList.Print()

	fmt.Println(log)
}
