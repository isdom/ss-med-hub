#!/bin/bash

export NACOS_ENDPOINT=
export NACOS_NAMESPACE=

export NACOS_DATAID=med-hub
export METER_DATAID=prometheus
export METER_GROUP=
export REDIS_DATAID=redis
export REDIS_GROUP=
export SD_DATAID=nacos-sd
export SD_GROUP=
export OSS_DATAID=oss
export OSS_GROUP=

export SLS_ENDPOINT=cn-beijing-intranet.log.aliyuncs.com
export SLS_PROJECT=
export SLS_LOGSTORE=
export SLS_TOPIC=med-hub-dev

export CPB_GROUP=DEFAULT_GROUP
export CPB_DATAID=sls-access.conf

export JVM_MEM=2048M
export JVM_DIRECT_MEM=256M
export JVM_PID_FILE=med-hub.pid
export JVM_BOOT_JAR=med-hub-1.0-SNAPSHOT.jar
export JVM_SPRING_PROFILE=dev