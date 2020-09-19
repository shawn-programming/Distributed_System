package main

import (
	"fmt"
	"time"

	"../membership"
)

var start = time.Now().Second()

func main() {

	var membershipList membership.MsList

	//membershipList = addTest(membershipList)

	//membershipList = removeTest(membershipList)

	membershipList = updateTest(membershipList)

}

func makeMember(start int, id string, ip string, count int) membership.Membership {
	elapsed := time.Now().Second() - start
	m := membership.CreateMembership(id, ip, count, elapsed)
	return m
}

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

func updateTest(inputList membership.MsList) membership.MsList {
	fmt.Println("------------------------update test---------------------------------------")

	var compareList membership.MsList

	member1 := makeMember(start, "1", "127.0.0.1:1234", 0)
	member2 := makeMember(start, "2", "127.0.0.1:1235", 0)
	member3 := makeMember(start, "3", "127.0.0.1:1236", 0)
	member4 := makeMember(start, "4", "127.0.0.1:1237", 1)

	elapsed := time.Now().Second() - start
	time.Sleep(time.Second * 2)
	compareList = compareList.Add(member1, elapsed)

	elapsed = time.Now().Second() - start
	time.Sleep(time.Second * 2)
	compareList = compareList.Add(member2, elapsed)

	elapsed = time.Now().Second() - start
	time.Sleep(time.Second * 2)
	compareList = compareList.Add(member3, elapsed)

	elapsed = time.Now().Second() - start
	time.Sleep(time.Second * 2)
	compareList = compareList.Add(member4, elapsed)

	fmt.Println("------compareList------")
	compareList.Print()

	fmt.Println("------inputList------")
	inputList = addTest(inputList)

	timeout := 10

	elapsed = time.Now().Second() - start
	fmt.Println("Current Time:", elapsed)

	var fail []membership.Id
	inputList = inputList.UpdateMsList(compareList, elapsed)

	fail = inputList.CheckFails(elapsed, timeout)

	fmt.Println("----------------------update output---------------------------------")
	inputList.Print()
	fmt.Println("----------------------failed processes---------------------------------")
	for _, f := range fail {
		f.Print()
	}

	return inputList

}
