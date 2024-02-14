#!/bin/bash
log_output () {
    echo -e "\033[;$1m$2\033[0m"
}
for i in {1..1}
do 
    # VERBOSE=1 go test -run ${INDEX} -race > output_${INDEX}.log
    # if [ "$?" != 0 ]; then 
    #     log_output 31 "FAIL Tests[${INDEX}] Round[${i}]"
    #     exit 1
    # else
    #     log_output 32 "PASS Tests[${INDEX}] Round[${i}]"
    #     rm -rf output_${INDEX}.log
    # fi
    go test -run=2A -race
    go test -run=2B -race
    go test -run=2C -race
    go test -run=2D -race
done