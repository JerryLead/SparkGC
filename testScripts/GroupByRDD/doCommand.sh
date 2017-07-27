#!/bin/sh

doCommand()
{
    hosts=`sed -n '/^[^#]/p' hostlist`
    for host in $hosts
        do
            echo ""
            echo HOST $host
            ssh $host "$@"
        done
    return 0
}

    if [ $# -lt 1 ]
    then
            echo "$0 cmd"
            exit
    fi
    doCommand "$@"
    echo "return from doCommand"
