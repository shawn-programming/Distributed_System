#!/bin/bash

echo "Installing Logly..."

# Check that all programs needed to run Logly are installed
# command -v go >/dev/null 2>&1 || { sudo yum install go -y }
# command -v python3 >/dev/null 2>&1 || { sudo yum install python3 -y}

# Todo: Ask for server ID
# python3 test_gen.py ID

echo "export GOPATH=$HOME/go" >> ~/.bashrc
source ~/.bashrc

echo "Done."
