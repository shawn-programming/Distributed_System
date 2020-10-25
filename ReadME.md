# Distributed System: FileSysmte



Our system has one leader node that stores all file meta-data within the system and processes all put/get requests made from clients. For leader failure, we used ring leader election protocol. 

## Install GO:
wget https://dl.google.com/go/go1.13.src.tar.gz
tar -C /usr/local -xzf go$VERSION.$OS-$ARCH.tar.gz
export PATH=$GOPATH:~/go

## Set up:
1. mkdir ~/go
2. cd ~/go
3. git clone https://gitlab.engr.illinois.edu/hl8/cs425.git
4. cd distributed_system
5. cd Process
6. cd distributed_file
7. rm -rf *
6. go build main.go


## Usage
1. ./main.go #VMnumber
2. -h to see possible commands 
