#!/bin/bash

source ~/env.sh

PIDFILE=~/pids/${JVM_PID_FILE}

if [ ! -f ${PIDFILE} ]
then
    nohup java -Xms${JVM_MEM} -Xmx${JVM_MEM} \
      -XX:+HeapDumpOnOutOfMemoryError \
      -XX:MaxDirectMemorySize=${JVM_DIRECT_MEM} \
      -Dio.netty.recycler.maxCapacity=0 \
      -Dio.netty.allocator.tinyCacheSize=0 \
      -Dio.netty.allocator.smallCacheSize=0 \
      -Dio.netty.allocator.normalCacheSize=0 \
      -Dio.netty.allocator.type=pooled \
      -Dio.netty.leakDetection.level=PARANOID \
      -Dio.netty.leakDetection.maxRecords=50 \
      -Dio.netty.leakDetection.acquireAndReleaseOnly=true \
      -jar ~/${JVM_BOOT_JAR} \
      --spring.profiles.active=${JVM_SPRING_PROFILE} \
      2>&1 &

    echo $! > $PIDFILE
else
    if [ ! -d /proc/$(cat ${PIDFILE}) ]
    then
       echo "warn: ${PIDFILE} process !NOT! exist, just rm pid file, please try run $0 again!"
       rm ${PIDFILE}
    else
       echo "error: ${JVM_BOOT_JAR} $(cat ${PIDFILE}) process already exist, Abort $0!"
    fi
fi