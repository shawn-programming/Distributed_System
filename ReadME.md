# Distributed System: FailStop


This Program uses an All-to-All and a Gossip style heart beating to detect a failure of a process in a distributed system.

## Install GO:
wget https://dl.google.com/go/go1.13.src.tar.gz
tar -C /usr/local -xzf go$VERSION.$OS-$ARCH.tar.gz
export PATH=$GOPATH:~/go

## Set up:
1. mkdir ~/go
2. cd ~/go
3. git clone https://gitlab.engr.illinois.edu/hl8/cs425.git
4. cd distributed_system
5. cd FailStop
6. go build main.go


## Usage
1. ./main.go #VMnumber
2. -h to see possible commands 
