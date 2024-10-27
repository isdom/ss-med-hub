#!/bin/bash

source ~/env.sh

PIDFILE=~/pids/asr-hub.pid

if [ ! -f ${PIDFILE} ]
then
    nohup java -Xms512m -Xmx512m \
      -XX:+HeapDumpOnOutOfMemoryError \
      -XX:MaxDirectMemorySize=128M \
      -Dio.netty.recycler.maxCapacity=0 \
      -Dio.netty.allocator.tinyCacheSize=0 \
      -Dio.netty.allocator.smallCacheSize=0 \
      -Dio.netty.allocator.normalCacheSize=0 \
      -Dio.netty.allocator.type=pooled \
      -Dio.netty.leakDetection.level=PARANOID \
      -Dio.netty.leakDetection.maxRecords=50 \
      -Dio.netty.leakDetection.acquireAndReleaseOnly=true \
      -jar ~/asr-hub-1.0-SNAPSHOT.jar \
      --spring.profiles.active=dev  \
      2>&1 &

    echo $! > $PIDFILE
else
    if [ ! -d /proc/$(cat ${PIDFILE}) ]
    then
       echo "warn: ${PIDFILE} process !NOT! exist, just rm pid file, please try run start_hub.sh again!"
       rm ${PIDFILE}
    else
       echo "error: asr-hub $(cat ${PIDFILE}) process already exist, Abort!"
    fi
fi
