#!/bin/bash
log_output () {
    echo -e "\033[;$1m$2\033[0m"
}
for i in {1..1}
do 
    # go test -run=${INDEX}
    # if [ "$?" != 0 ]; then 
    #     log_output 31 "FAIL Tests[${INDEX}] Round[${i}]"
    #     exit 1
    # else
    #     log_output 32 "PASS Tests[${INDEX}] Round[${i}]"
    # fi
    go test -run=2A
    go test -run=2B
    go test -run=2C
    go test -run=2D
done