package node

import (
	ms "../Membership"
)

/*
Node for a processor
*/
type Node struct {
	id        ms.Id
	localTime int
	timeOut   int
	msList    ms.MsList
}

func createNode(idNum, IPAddress string, localTime, timeOut int) ms.Membership {
	membership := ms.CreateMembership(idNum, IPAddress, localTime, timeOut)
	return membership
}
