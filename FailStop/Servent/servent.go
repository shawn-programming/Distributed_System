package main

import "flag"

//membership
//node

// The main system
func main() {
	isIntroducer := flag.Bool("isIntroducer", false) // check whether this node is introducer or not
	portNum := flag.String("portNum", "1234")        // get port number
	
	serverID := generateID()
	flag.Parse()

	go initMemberList()

	if !(*introducer) {
		connectToCluster("IntroducerAddr")
	}

	go for{
		getPings() // --> InputList net.liten
	}
	
	go for{
		if introducer{
			go for{
				newList = {}
	
				if newList is not empty{
					make it join
				}
			}
		}
		incrementLocalTime(InputList)
		sendPings()		// write(membership)

	}
	
	go sendMessage() //server

	go listenOnPort() //client

}

//initialize MembershipList of current server
func initMemberList() {

}

// send membershipList to all the nodes present in the cluster
func sendMessage() {

}

// request introducer to add current server to the cluster
func connectToCluster() {
}

// Listen to incoming messages (membershipList)
func listenOnPort() {

	// perform different action depends on types of messages
}

// ### utility ###
// generate unique serverID
func generateID() {

}

// check err and exit if occured
func checkError(err Error) {
}
