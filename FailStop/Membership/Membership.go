package membership

import (
	"fmt"
	"sort"
)

type Id struct {
	IdNum     string
	IPAddress string
}
type Membership struct {
	ID        Id
	count     int
	localTime int
}

type MsList struct {
	list []Membership
}

/*
Id.print()
	RETURN: None

	print information in Id
*/
func (ID Id) Print() {
	fmt.Println(ID.IdNum, " : ", ID.IPAddress)
}

/*
member.print()
	RETURN: None

	print information in member
*/
func (member Membership) Print() {
	member.ID.Print()
	fmt.Println("Count: ", member.count)
	fmt.Println("localTime: ", member.localTime)
}

/*
MsList.print()
	RETURN: None

	print information in MsList
*/
func (members MsList) Print() {
	for _, member := range members.list {
		member.Print()
		fmt.Println("")
	}
}

/*
Membership constructor
	RETURN: conrstructed Membership
*/
func CreateMembership(IdNum string, IPAddress string, count int, locatime int) Membership {
	thisID := Id{IdNum, IPAddress}
	thisMembership := Membership{thisID, count, locatime}

	return thisMembership
}

/*
MsList.add(member)
	RETURN: NONE

add member to the MsList, and sort tthe MsList.List by its IdNum
*/
func (members MsList) Add(member Membership) MsList {
	members.list = append(members.list, member)
	sort.SliceStable(members.list, func(i, j int) bool {
		return members.list[i].ID.IdNum < members.list[j].ID.IdNum
	})
	return members
}

/*
MsList.remove(Id)
	RETURN: NONE

	find the meber with corresponding Id and remove it
*/
func (members MsList) Remove(targetID Id) MsList {
	for i, member := range members.list {
		if member.ID == targetID {
			members.list = append(members.list[:i], members.list[i+1:]...)
		}
	}
	fmt.Println("Could not find the ID: ", targetID)
	return members

}

/*
MsList.update(toCompare, currLocalTime, timeOut)
	RETURN: LIST OF FAILED MEMBER'S ID

	compare MsList with toCompare,
	for each member, if counter incrememnted, update it
	otherwise, check whether if failed by checking if currTime - localTime > timeOut
	if failed, add that member's Id to the failList
*/

func (members MsList) UpdateMsList(toCompare MsList, currTime int, timeOut int) []Id {
	var failList []Id

	inputList := toCompare.list
	for i, member := range members.list {
		if member.count < inputList[i].count {
			member.count = inputList[i].count
			member.localTime = currTime
		} else if currTime-member.localTime > timeOut {
			failList = append(failList, member.ID)
			fmt.Println("Failure dected: ")
			member.Print()
		}
	}
	return failList
}
