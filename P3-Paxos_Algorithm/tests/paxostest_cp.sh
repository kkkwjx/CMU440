#!/bin/bash

if [ -z $GOPATH ]; then
    echo "FAIL: GOPATH environment variable is not set"
    exit 1
fi

if [ -n "$(go version | grep 'darwin/amd64')" ]; then    
    GOOS="darwin_amd64"
elif [ -n "$(go version | grep 'linux/amd64')" ]; then
    GOOS="linux_amd64"
else
    echo "FAIL: only 64-bit Mac OS X and Linux operating systems are supported"
    exit 1
fi

# Build the student's paxos node implementation.
# Exit immediately if there was a compile-time error.
go install github.com/cmu440-F16/paxosapp/runners/prunner
if [ $? -ne 0 ]; then
   echo "FAIL: code does not compile"
   exit $?
fi

# Build the test binary to use to test the student's paxos node implementation.
# Exit immediately if there was a compile-time error.
go install github.com/cmu440-F16/paxosapp/tests/paxostest
if [ $? -ne 0 ]; then
   echo "FAIL: code does not compile"
   exit $?
fi

# Pick random ports between [10000, 20000).
NODE_PORT0=$(((RANDOM % 10000) + 10000))
NODE_PORT1=$(((RANDOM % 10000) + 10000))
NODE_PORT2=$(((RANDOM % 10000) + 10000))
TESTER_PORT=$(((RANDOM % 10000) + 10000))
PROXY_PORT=$(((RANDOM & 10000) + 10000))
PAXOS_TEST=$GOPATH/bin/paxostest
PAXOS_NODE=$GOPATH/bin/prunner
ALL_PORTS="${NODE_PORT0},${NODE_PORT1},${NODE_PORT2}"
echo ${ALL_PORTS}
echo ${PROXY_PORT}
##################################################

# Start paxos node.
${PAXOS_NODE} -myport=${NODE_PORT0} -ports=${ALL_PORTS} -N=3 -id=0 -pxport=${PROXY_PORT} -proxy=0 2> /dev/null &
PAXOS_NODE_PID0=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT1} -ports=${ALL_PORTS} -N=3 -id=1 -pxport=${PROXY_PORT} -proxy=0 2> /dev/null &
PAXOS_NODE_PID1=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT2} -ports=${ALL_PORTS} -N=3 -id=2 -pxport=${PROXY_PORT} -proxy=0 2> /dev/null &
PAXOS_NODE_PID2=$!
sleep 1

# Start paxostest. 
${PAXOS_TEST} -port=${TESTER_PORT} -paxosports=${ALL_PORTS} -N=3 -nodeport=${NODE_PORT0} -pxport=${PROXY_PORT}

# Kill paxos node.
kill -9 ${PAXOS_NODE_PID0}
kill -9 ${PAXOS_NODE_PID1}
kill -9 ${PAXOS_NODE_PID2}
wait ${PAXOS_NODE_PID0} 2> /dev/null
wait ${PAXOS_NODE_PID1} 2> /dev/null
wait ${PAXOS_NODE_PID2} 2> /dev/null
