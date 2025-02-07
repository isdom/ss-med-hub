#!/bin/bash

source ~/env.sh

PIDFILE=~/pids/${JVM_PID_FILE}

if [ ! -f ${PIDFILE} ]
then
    echo "warn: could not find file ${PIDFILE}"
else
    if [ ! -d /proc/$(cat ${PIDFILE}) ]
    then
       echo "warn: ${PIDFILE} process !NOT! exist, just rm pid file"
    else
        kill $(cat ${PIDFILE})
    fi
    rm ${PIDFILE}
    echo STOPPED
fi