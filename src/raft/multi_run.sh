#!/bin/bash
log_output () {
    echo -e "\033[;$1m$2\033[0m"
}
for i in {1..10}
do 
    VERBOSE=1 go test -run ${INDEX} > output_${INDEX}.log
    if [ "$?" != 0 ]; then 
        log_output 31 "FAIL Tests[${INDEX}] Round[${i}]"
        exit 1
    else
        log_output 32 "PASS Tests[${INDEX}] Round[${i}]"
        rm -rf output_${INDEX}.log
    fi
    # go test -run=2A
    # go test -run=2B
    # go test -run=2C
    # go test -run=2D
done