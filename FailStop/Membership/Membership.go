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
	Count     int
	localTime int
	Failed    bool
}

type MsList struct {
	List []Membership
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
	fmt.Println("Count: ", member.Count)
	fmt.Println("localTime: ", member.localTime)
	fmt.Println("Failed: ", member.Failed)
}

/*
MsList.print()
	RETURN: None

	print information in MsList
*/
func (members MsList) Print() {
	for _, member := range members.List {
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
	thisMembership := Membership{thisID, count, locatime, false}

	return thisMembership
}

/*
Mebership EqualTo
	RETURN: True if member's IdNum is less than toCompare's IdNum

*/
func (member Membership) EqualTo(toCompare Membership) bool {
	return member.ID.IdNum == toCompare.ID.IdNum
}

/*
Mebership lessThan
	RETURN: True if member's IdNum is less than toCompare's IdNum

*/
func (member Membership) lessThan(toCompare Membership) bool {
	return member.ID.IdNum < toCompare.ID.IdNum
}

/*
Mebership greaterThan
	RETURN: True if member's IdNum is less than toCompare's IdNum

*/
func (member Membership) greaterThan(toCompare Membership) bool {
	return member.ID.IdNum > toCompare.ID.IdNum
}

/*
MsList.add(member)
	RETURN: NONE

add member to the MsList, and sort tthe MsList.List by its IdNum
*/
func (members MsList) Add(member Membership, local int) MsList {
	member.localTime = local
	members.List = append(members.List, member)
	sort.SliceStable(members.List, func(i, j int) bool {
		return members.List[i].lessThan(members.List[j])
	})
	return members
}

/*
MsList.remove(Id)
	RETURN: NONE

	find the meber with corresponding Id and remove it
*/
func (members MsList) Remove(targetID Id) MsList {
	for i, member := range members.List {
		if member.ID == targetID {
			fmt.Println("Removing Member: ")
			member.ID.Print()
			members.List = append(members.List[:i], members.List[i+1:]...)
			return members
		}
	}
	fmt.Println("Could not find the ID: ", targetID)
	return members

}

/*
MsList.update(toCompare, currLocalTime, timeOut)
	RETURN: List OF FAILED MEMBER'S ID

	compare MsList with toCompare,
	for each member, if counter incrememnted, update it
	otherwise, check whether if failed by checking if currTime - localTime > timeOut
	if failed, add that member's Id to the failList
*/
func (members MsList) UpdateMsList(toCompare MsList, currTime int, selfID Id) MsList {
	// fmt.Println("------UpdateMsList-------")
	inputList := toCompare.List

	for i, member := range members.List {
		// fmt.Println("---------------ID-----------------------")
		// selfID.Print()
		// member.ID.Print()
		// fmt.Print("-----------------------------------------")
		if member.ID.IdNum == selfID.IdNum {
			// fmt.Println("Found")
			members.List[i].Count++
			members.List[i].localTime = currTime
		}
	}

	for _, input := range inputList {
		Found, idx := members.Find(input)
		// if input member is not in current MsList, and it is not failed
		// meaning this is a new member
		if !Found {
			if !input.Failed {
				fmt.Println("In UpdateMsList Phase, Member is not found. This should never happen")
			} else {
				continue
			}

		} else if members.List[idx].Count < input.Count {
			// if this member has a fresher counter, update it
			members.List[idx].Count = input.Count
			members.List[idx].localTime = currTime
		}
	}
	return members
}

func (members MsList) CheckFails(currTime int, timeOut int) (MsList, []Id) {
	var removeList []Id

	for i, member := range members.List {
		if currTime-member.localTime > timeOut { // local time exceeds timeout_fail
			if members.List[i].Failed == false {
				fmt.Println("Failure detected: ")
				members.List[i].Print()
				members.List[i].Failed = true
			}
		}

		if currTime-member.localTime > (timeOut * 2) { //local time exceeds time_cleanup
			members.List[i].Failed = true
			removeList = append(removeList, member.ID)
		}
	}

	return members, removeList
}

/*
	func (msList MsList) CheckMember(toCompare MsList) MsList
	RETURN: msList

	compare msList with the input.
	If the input misses any of member the msList has, remove it from the msList
*/
/*
func (msList MsList) CheckMember(toCompare MsList) MsList {
	toRemove := []Id{}
	j := 0

	for _, membership := range msList.List {
		if j > len(toCompare.List) {
			break
		}
		if membership.EqualTo(toCompare.List[j]) {
			j++
		} else if membership.lessThan(toCompare.List[j]) {
			toRemove = append(toRemove, membership.ID)
		}
	}

	for _, ID := range toRemove {
		msList.Remove(ID)
		fmt.Print("Removed: ")
		ID.Print()
	}

	return msList
}
*/

func (msList MsList) CheckMembers(toCompare MsList, currTime int, timeout int) MsList {
	for _, inputMember := range toCompare.List {
		exist, _ := msList.Find(inputMember)
		if !exist { // member 가 input 에 없으면
			if !inputMember.Failed { // fail 이 아닐경우
				msList = msList.Add(inputMember, currTime)
			}
		}
	}

	return msList
}

func (msList MsList) Find(member Membership) (bool, int) {

	for i, m := range msList.List {
		if member.EqualTo(m) {
			return true, i
		}
	}
	return false, -1

}
