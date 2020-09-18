package main

import (
	"fmt"
	"time"

	membership "../Membership"
)

func main() {
	count := 0
	t := time.Now() // start time
	startsec := t.Second()

	timeOut := 10

	t = time.Now()
	elapsed := t.Second() - startsec

	member1 := membership.CreateMembership("1", "127.0.0.1:1234", count, elapsed)

	time.Sleep(2 * time.Second)
	t = time.Now()
	elapsed = t.Second() - startsec
	member2 := membership.CreateMembership("2", "127.0.0.3:1234", count, elapsed)

	time.Sleep(1 * time.Second)
	t = time.Now()
	elapsed = t.Second() - startsec
	member3 := membership.CreateMembership("3", "127.0.0.3:1234", count, elapsed)

	time.Sleep(3 * time.Second)
	t = time.Now()
	elapsed = t.Second() - startsec

	member4 := membership.CreateMembership("4", "127.0.0.3:1234", count, elapsed)

	//check eahc Membership
	fmt.Println("Printing each memebrs....")
	member1.Print()
	member2.Print()
	member3.Print()
	member4.Print()

	//create list and add all members
	var membershipList membership.MsList

	membershipList = membershipList.Add(member1)
	membershipList = membershipList.Add(member2)
	membershipList = membershipList.Add(member3)
	membershipList = membershipList.Add(member4) //다시 ㄲ ? 머지ㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋ

	//output Membership list
	fmt.Println("Printing all memebrs of membereship list...")
	membershipList.Print()

	//remove member1
	fmt.Println("After removing member1 ...")
	id1 := membership.Id{"1", "127.0.0.1:1234"}
	membershipList = membershipList.Remove(id1)
	membershipList.Print()

	//remove unknown member
	fmt.Println("After removing unknown member...")
	unknown_id := membership.Id{"3", "127.0.0.1:1234"}
	membershipList = membershipList.Remove(unknown_id)
	membershipList.Print()

	fmt.Println("Updating membership List...")
	for {
		t = time.Now()
		elapsed = t.Second() - startsec

		membershipList.UpdateMsList(membershipList, elapsed, timeOut)
	}
}

//  일단 //일단컴파일 해ㅐ봄
