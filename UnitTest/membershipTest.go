package UnitTest

import (
	"fmt"
	"time"
	"Membership"
)

func main() {
	count := 0
	start := time.Now() // start time
	timeOut := 10

	t := time.Now()
	elapsed := t.Sub(start)
	member1 := Membership.createMembership{"1", "127.0.0.1:1234", count, elapsed}
	
	
	time.Sleep(2 * time.Second)
	elapsed = t.Sub(start)
	member2 := Membership.createMembership{"3", "127.0.0.3:1234", count, elapsed}

	time.Sleep(1 * time.Second)
	elapsed = t.Sub(start)
	member3 := Membership.createMembership{"3", "127.0.0.3:1234", count, elapsed}

	time.Sleep(3 * time.Second)
	elapsed = t.Sub(start)
		
	member4 := Membership.createMembership{"3", "127.0.0.3:1234", count, elapsed}

	//check eahc Membership
	fmt.Println("Printing each memebrs....")
	member1.print()
	member2.print()
	member3.print()
	member4.print()

	//create list and add all members
	var MembershipList MsList
	membershipList.add(member1)
	membershipList.add(member2)
	membershipList.add(member3)
	membershipList.add(member4)

	//output Membership list
	fmt.Println("Printing all memebrs of membereship list...")
	membershipList.print()

	//remove member1
	fmt.Println("After removing member1 ...")
	id1 := Id{"1", "127.0.0.1:1234"}
	membershipList.remove(id1)
	membershipList.print()

	//remove unknown member
	fmt.Println("After removing unknown member...")
	unknown_id := Id{"3", "127.0.0.1:1234"}
	membershipList.remove(unknown_id)
	membershipList.print()

	fmt.Println("Updating membership List...")
	for {
		t = time.Now()
		elapsed = t.Sub(start)

		membershipList.updateMsList(membershipList, elapsed, 10)
	}
}
