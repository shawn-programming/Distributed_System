package membership

import (
	"fmt"
	"sort"
	"strconv"
)

/*
 Struct for ID + IPAddress
*/
type Id struct {
	IdNum     string
	IPAddress string
}

/*
Struct for a Membership
*/
type Membership struct {
	ID        Id
	Count     int
	localTime int
	Failed    bool
}

/*
Struct for a List of Memberships
*/
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
Id.printLog()
	RETURN: None

	print information in Id
*/
func (ID Id) PrintLog() string {
	return ID.IdNum + " : " + ID.IPAddress
}

/*
member.print()
	RETURN: log string

	print information in member
*/
func (member Membership) Print() {
	member.ID.Print()
	fmt.Println("Count: ", member.Count)
	fmt.Println("localTime: ", member.localTime)
	fmt.Println("Failed: ", member.Failed)
}

/*
member.printLog()
	RETURN: log string

	print information in member
*/
func (member Membership) PrintLog() string {
	var failed string
	if member.Failed {
		failed = "True\n"
	} else {
		failed = "False\n"
	}
	log := "ID: " + member.ID.IdNum + "\n"
	log += "Count: " + strconv.Itoa(member.Count) + "\n"
	log += "localTime: " + strconv.Itoa(member.localTime) + "\n"
	log += "Failed: " + failed
	return log

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
MsList.printLog()
	RETURN: log

	print information in MsList
*/
func (members MsList) PrintLog() string {
	var log string
	for _, member := range members.List {
		log += member.PrintLog()
	}
	return log
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
MsList.Add(member)
	RETURN: updated membership List

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
MsList.Remove(Id)
	RETURN: updated membership List, log data

	find the meber with corresponding Id and remove it
*/
func (members MsList) Remove(targetID Id) (MsList, string) {
	var log string
	for i, member := range members.List {
		if member.ID == targetID {
			log += "\nRemoving Member: \n"
			log += member.PrintLog()
			member.ID.Print()
			members.List = append(members.List[:i], members.List[i+1:]...)
			return members, log
		}
	}
	return members, log

}

/*
MsList.UpdateMsList(toCompare MsList, currTime int, selfID Id) MsList	RETURN: List OF FAILED MEMBER'S ID
	Return: updated membership List

	compare MsList with toCompare,
	for each member, if counter incrememnted, update it
	otherwise, check whether if failed by checking if currTime - localTime > timeOut
	if failed, add that member's Id to the failList
*/
func (members MsList) UpdateMsList(toCompare MsList, currTime int, selfID Id) MsList {
	inputList := toCompare.List

	for i, member := range members.List {
		if member.ID.IdNum == selfID.IdNum {
			members.List[i].Count++
			members.List[i].localTime = currTime
		}
	}

	for _, input := range inputList {
		Found, idx := members.Find(input)
		if !Found {
			if !input.Failed {
			} else {
				continue
			}
		} else if members.List[idx].Count < input.Count && members.List[idx].Failed == false {
			members.List[idx].Count = input.Count
			members.List[idx].localTime = currTime
		}
	}
	return members
}

/*
MsList.CheckFails(currTime int, timeOut int)
	RETURN: updated MsList, list of removed IDs, log data

	checks each member's localTime and updates its Failed Status
*/
func (members MsList) CheckFails(currTime int, timeOut int) (MsList, []Id, []Id, string) {
	var failList []Id
	var removeList []Id
	var log string
	for i, member := range members.List {
		if currTime-member.localTime > timeOut { // local time exceeds timeout_fail
			if members.List[i].Failed == false {
				fmt.Println("Failure detected: ")
				members.List[i].Failed = true
				failList = append(failList, members.List[i].ID)
				members.List[i].Print()
				log = "\nFailure detected: \n"
				log += members.List[i].PrintLog()

			}
		}

		if currTime-member.localTime > (timeOut * 2) { //local time exceeds time_cleanup
			members.List[i].Failed = true
			removeList = append(removeList, member.ID)
		}
	}

	return members, failList, removeList, log
}

/*
 MsList.CheckMembers(toCompare MsList, currTime int, timeout int) (MsList, string)	RETURN: updated MsList, log data
	Return: updated  membership List, log data

	compares the current membership List against input membership List
	and updates the memership List by adding unseen members.
*/
func (msList MsList) CheckMembers(toCompare MsList, currTime int, timeout int) (MsList, string) {
	var log string
	for _, inputMember := range toCompare.List {
		exist, _ := msList.Find(inputMember)
		if !exist {
			if !inputMember.Failed {
				msList = msList.Add(inputMember, currTime)
				log = "\nNew Member Added: \n"
				log += inputMember.PrintLog()
			}
		}
	}

	return msList, log
}

/*
MsList.Find(member Membership)
	RETURN: false in not found, true if found

	check whether input member is present in the current membership List.
*/
func (msList MsList) Find(member Membership) (bool, int) {

	for i, m := range msList.List {
		if member.EqualTo(m) {
			return true, i
		}
	}
	return false, -1

}
