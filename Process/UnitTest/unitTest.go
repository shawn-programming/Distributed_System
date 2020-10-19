package main

import (
	"fmt"
	"time"

	membership "../Membership"
	nodeTest "../UnitTest/nodeTest"
)

var start = time.Now().Second()

func main() {

	// var membershipList membership.MsList

	//membershipList = addTest(membershipList)

	//membershipList = removeTest(membershipList)

	//UpdateMsListTest()

	//checkfailsTest()
	// checkmembersTest()

	nodeTest.IncrementLocalTimeTest()

}

/*
	Member constructor for unit testing
*/

func makeMember(start int, id string, ip string, count int) membership.Membership {
	elapsed := time.Now().Second() - start
	m := membership.CreateMembership(id, ip, count, elapsed)
	return m
}

/*
	Testing Add
*/
func addTest(inputList membership.MsList) membership.MsList {
	fmt.Println("----------------------------addtest-------------------------------------")
	member1 := makeMember(start, "1", "127.0.0.1:1234", 0)
	time.Sleep(time.Second * 2)
	member2 := makeMember(start, "2", "127.0.0.1:1235", 0)
	time.Sleep(time.Second * 2)
	member3 := makeMember(start, "3", "127.0.0.1:1236", 0)
	time.Sleep(time.Second * 2)
	member4 := makeMember(start, "4", "127.0.0.1:1237", 0)

	inputList = inputList.Add(member1, 7)
	inputList = inputList.Add(member2, 8)
	inputList = inputList.Add(member3, 2)
	inputList = inputList.Add(member4, 0)
	fmt.Println("----------------------------after adding-------------------------------")

	inputList.Print()
	return inputList
}

/*
	Testing Remove
*/
func removeTest(inputList membership.MsList) membership.MsList {
	fmt.Println("----------------------------removetest---------------------------------")

	fmt.Println("----------------------------before-------------------------------------")
	inputList.Print()

	fmt.Println("----------------------------after-------------------------------------")
	Id1 := membership.Id{"1", "127.0.0.1:1234"}
	inputList = inputList.Remove(Id1)
	inputList.Print()

	fmt.Println("----------------------------before------------------------------------")
	inputList.Print()
	unknownID := membership.Id{"2", "127.0.0.1:1234"}
	inputList = inputList.Remove(unknownID)

	fmt.Println("----------------------------after-------------------------------------")
	inputList.Print()
	return inputList
}

/*
	Testing UpdateList()
*/
func UpdateMsListTest() {
	fmt.Println("------------------------update test---------------------------------------")

	var compareList membership.MsList

	member1 := makeMember(start, "1", "127.0.0.1:1234", 0)
	member2 := makeMember(start, "2", "127.0.0.1:1235", 1)
	member3 := makeMember(start, "3", "127.0.0.1:1236", 0)
	member4 := makeMember(start, "4", "127.0.0.1:1237", 1)

	compareList = compareList.Add(member1, 1)
	compareList = compareList.Add(member2, 2)
	compareList = compareList.Add(member3, 3)
	compareList = compareList.Add(member4, 4)

	fmt.Println("------compareList------")
	compareList.Print()

	var inputList membership.MsList

	input2 := makeMember(start, "2", "127.0.0.1:1235", 1)
	input3 := makeMember(start, "3", "127.0.0.1:1236", 1)
	input4 := makeMember(start, "4", "127.0.0.1:1237", 5)
	input5 := makeMember(start, "5", "127.0.0.1:1238", 1)
	input6 := makeMember(start, "6", "127.0.0.1:1239", 1)
	input6.Failed = true

	inputList = inputList.Add(input2, 6)
	inputList = inputList.Add(input3, 7)
	inputList = inputList.Add(input4, 14)
	inputList = inputList.Add(input5, 4)
	inputList = inputList.Add(input6, 4)

	fmt.Println("------inputList------")
	inputList.Print()

	fmt.Println("----------------------update output---------------------------------")
	compareList = compareList.UpdateMsList(inputList, 10)
	compareList.Print()

}

/*
	Testing CheckFails
*/

func checkfailsTest() {
	fmt.Println("----------running checkfails Test----------")
	var testList membership.MsList

	member1 := makeMember(start, "1", "127.0.0.1:1234", 0)
	member2 := makeMember(start, "2", "127.0.0.1:1235", 0)
	member3 := makeMember(start, "3", "127.0.0.1:1236", 0)
	member4 := makeMember(start, "4", "127.0.0.1:1237", 0)

	//time.Sleep(time.Second * 2) // 2
	elapsed := 2
	testList = testList.Add(member1, elapsed)

	//time.Sleep(time.Second * 2) //4
	elapsed = 4
	testList = testList.Add(member2, elapsed)

	//time.Sleep(time.Second * 2) // 6
	elapsed = 6
	testList = testList.Add(member3, elapsed)

	//time.Sleep(time.Second * 2) //8
	elapsed = 8
	testList = testList.Add(member4, elapsed)

	fmt.Println("----------TestList----------")
	testList.Print()

	currTime := 10
	timeout := 3 // 1 remove 3 fail

	testList, removeList := testList.CheckFails(currTime, timeout)

	fmt.Println("----------leftList----------")
	testList.Print()

	fmt.Println("----------removed members----------")

	for _, r := range removeList {
		r.Print()
	}

}

/*
	testing checkmembers()
*/

func checkmembersTest() {

	var testList membership.MsList
	var compareList membership.MsList

	member1 := makeMember(start, "1", "127.0.0.1:1234", 0)
	member2 := makeMember(start, "2", "127.0.0.1:1235", 0)
	member3 := makeMember(start, "3", "127.0.0.1:1236", 0)
	member4 := makeMember(start, "4", "127.0.0.1:1237", 0)

	//time.Sleep(time.Second * 2) // 2
	elapsed := 2
	testList = testList.Add(member1, elapsed)
	compareList = compareList.Add(member1, elapsed)

	//time.Sleep(time.Second * 2) //4
	elapsed = 4
	testList = testList.Add(member2, elapsed)
	compareList = compareList.Add(member2, elapsed)

	//time.Sleep(time.Second * 2) // 6
	elapsed = 6
	//testList = testList.Add(member3, elapsed)
	compareList = compareList.Add(member3, elapsed)

	//time.Sleep(time.Second * 2) //8
	elapsed = 8
	//testList = testList.Add(member4, elapsed)
	member4.Failed = true
	compareList = compareList.Add(member4, elapsed)

	fmt.Println("----------testList----------")
	testList.Print()

	fmt.Println("----------compareList----------")
	compareList.Print()

	currTime := 10
	timeout := 3
	testList = testList.CheckMembers(compareList, currTime, timeout)

	fmt.Println("----------result----------")
	testList.Print()
}
