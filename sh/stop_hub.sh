#!/bin/bash

PIDFILE=~/pids/med-hub.pid

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
