#!/bin/bash
# Scripts to manage Spark standalone daemons
# Bowen Yu <stevenybw@hotmail.com>

set -u

# Directory to the bash

RELATIVE_SCRIPT_DIR=$(dirname $BASH_SOURCE)
cd $RELATIVE_SCRIPT_DIR
SCRIPT_DIR=$(pwd)

# Several typical configurations

# Run locally: 1 node, 2 numa-node, 6 executors, (8 cores + 30 GB) for each executor
LOCAL="1 2 6 8 30g"

# Run distributed: 8 nodes, 2 numa-node, 6 executors, (8 cores + 30 GB) for each executor
DIST="8 2 6 8 30g"

# ================= TODO: Sync the following configuration =================

# $USER is current user, for example, ybw

EVENT_LOG_DIR="hdfs://bic07.lab.pacman-thu.org:8020/shared/spark-log"

HDFS_PREFIX="hdfs://bic07.lab.pacman-thu.org:8020/user/${USER}"

# Spark work dir stores the staged files such as the program and its dependencies
SPARK_WORK_DIR="/mnt/disk1/${USER}/spark-work-dir"

# Use this for NVMe
# SPARK_LOCAL_DIR="/mnt/ssd0/${USER}/local-dir,/mnt/ssd1/${USER}/local-dir,/mnt/ssd2/${USER}/local-dir,/mnt/ssd3/${USER}/local-dir,/mnt/ssd4/${USER}/local-dir,/mnt/ssd5/${USER}/local-dir,/mnt/ssd6/${USER}/local-dir,/mnt/ssd7/${USER}/local-dir"

# Use this for HDD
SPARK_LOCAL_DIR="/mnt/disk1/${USER}/spark-local-dir,/mnt/disk2/${USER}/spark-local-dir,/mnt/disk3/${USER}/spark-local-dir,/mnt/disk4/${USER}/spark-local-dir,/mnt/disk5/${USER}/spark-local-dir,/mnt/disk6/${USER}/spark-local-dir"

export SPARK_HOME="/home/${USER}/Software/spark-2.4.3-bin-hadoop2.7"

# All hosts potentially involved in this experiment (relavant processes will be killed by stop_slaves)
AVAILABLE_HOSTLIST=( "bic01" "bic02" "bic03" "bic04" "bic05" "bic07" "bic08" "bic09" "bic06" )

# Hosts selected to be a slave (the first P processes will be chosen as slaves)
SLAVES_HOSTLIST=( "bic01" "bic02" "bic03" "bic04" "bic05" "bic07" "bic08" "bic09" )

# Basic flags to launch Spark
BASIC_SPARK_CONF="--conf spark.scheduler.minRegisteredResourcesRatio=1.0 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryoserializer.buffer.max=2040m --conf spark.driver.maxResultSize=0 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=$EVENT_LOG_DIR --conf spark.local.dir=$SPARK_LOCAL_DIR"

# ==========================================================================

MASTER=$(hostname)

function check_file() {
    path=$1
    if [ -d "$path" ]; then
        echo "[OK] Directory $path exists"
    else
        echo "[ERROR] Directory $path not found, please create it"
    fi
}

# Used for checking the environemtn
function check_environment() {
    if [ -d "$SPARK_HOME" ]; then
        echo "[OK] Spark home dir $SPARK_HOME"
    else
        echo "[ERROR] Spark home dir $SPARK_HOME not found, please download from Spark website"
    fi
    check_file $SPARK_WORK_DIR
    for file in $(echo $SPARK_LOCAL_DIR | sed "s/,/ /g"); do 
        check_file $file
    done
}

# Start the master
function start_master() {
    SPARK_HOME=${SPARK_HOME} ${SPARK_HOME}/sbin/start-master.sh
}

# Stop the master
function stop_master() {
    ${SPARK_HOME}/sbin/stop-master.sh
    sleep 1
    ps -aux | grep "org.apache.spark.deploy.master.Master" | grep -v "grep" | tr -s " " | cut -d " " -f 2 | xargs -i kill -9 {}
    ps -aux | grep "org.apache.spark.deploy.master.Master" | grep -v "grep" | tr -s " " | cut -d " " -f 2
}

# Start the Spark slaves
function start_slaves() {
    num_nodes=$1
    numa_nodes_per_node=$2
    container_per_node=$3
    cores_per_container=$4
    memory_per_container=$5
    
    #hostlist=${AVAILABLE_HOSTLIST[@]:0:$num_nodes}
    hostlist=${SLAVES_HOSTLIST[@]:0:$num_nodes}
    remote_cmd="SPARK_HOME=${SPARK_HOME} SPARK_WORKER_INSTANCES=${container_per_node} SPARK_NUMA_NODES=${numa_nodes_per_node} ${SCRIPT_DIR}/sbin/start-slave-numabind.sh -c ${cores_per_container} -m ${memory_per_container} -i "'$(hostname -f)'" -d ${SPARK_WORK_DIR} spark://${MASTER}:7077"
    clush -w $(echo $hostlist | sed "s/ /,/g") ${remote_cmd}
}

# Kill all the slaves in the AVAILABLE_HOSTLIST
function stop_slaves() {
    clush -w $(echo ${AVAILABLE_HOSTLIST[@]} | sed "s/ /,/g") 'ps -aux | grep "org.apache.spark.deploy.worker.Worker" | grep -v "grep" | tr -s " " | cut -d " " -f 2 | xargs -i kill {}'
    clush -w $(echo ${AVAILABLE_HOSTLIST[@]} | sed "s/ /,/g") 'ps -aux | grep "org.apache.spark.executor.CoarseGrainedExecutorBackend" | grep -v "grep" | tr -s " " | cut -d " " -f 2 | xargs -i kill {}'
    clush -w $(echo ${AVAILABLE_HOSTLIST[@]} | sed "s/ /,/g") 'ps -aux | grep "SparkSubmit" | grep -v "grep" | tr -s " " | cut -d " " -f 2 | xargs -i kill {}'
    ps -aux | grep "SparkSubmit" | grep -v "grep" | tr -s " " | cut -d " " -f 2 | xargs -i kill {}
    sleep 1
    clush -w $(echo ${AVAILABLE_HOSTLIST[@]} | sed "s/ /,/g") 'ps -aux | grep "org.apache.spark.deploy.worker.Worker" | grep -v "grep" | tr -s " " | cut -d " " -f 2 | xargs -i kill -9 {}'
    clush -w $(echo ${AVAILABLE_HOSTLIST[@]} | sed "s/ /,/g") 'ps -aux | grep "org.apache.spark.executor.CoarseGrainedExecutorBackend" | grep -v "grep" | tr -s " " | cut -d " " -f 2 | xargs -i kill {}'
    clush -w $(echo ${AVAILABLE_HOSTLIST[@]} | sed "s/ /,/g") 'ps -aux | grep "SparkSubmit" | grep -v "grep" | tr -s " " | cut -d " " -f 2 | xargs -i kill -9 {}'
    ps -aux | grep "SparkSubmit" | grep -v "grep" | tr -s " " | cut -d " " -f 2 | xargs -i kill -9 {}
    sleep 1
    clush -w $(echo ${AVAILABLE_HOSTLIST[@]} | sed "s/ /,/g") 'ps -aux | grep "org.apache.spark.deploy.worker.Worker" | grep -v "grep" | tr -s " "'
    clush -w $(echo ${AVAILABLE_HOSTLIST[@]} | sed "s/ /,/g") 'ps -aux | grep "org.apache.spark.executor.CoarseGrainedExecutorBackend" | grep -v "grep" | tr -s " " | cut -d " " -f 2 | xargs -i kill {}'
    clush -w $(echo ${AVAILABLE_HOSTLIST[@]} | sed "s/ /,/g") 'ps -aux | grep "SparkSubmit" | grep -v "grep" | tr -s " "'
    ps -aux | grep "SparkSubmit" | grep -v "grep" | tr -s " "
}

# Show the command to launch spark shell
function show_spark_shell_command() {
    num_nodes=$1
    numa_nodes_per_node=$2
    container_per_node=$3
    cores_per_container=$4
    memory_per_container=$5
    total_cores=$(echo $num_nodes*$container_per_node*$cores_per_container | bc)
    
    echo SPARK_HOME=$SPARK_HOME $SPARK_HOME/bin/spark-shell --master spark://${MASTER}:7077 $BASIC_SPARK_CONF --conf spark.default.parallelism=$total_cores --driver-memory $memory_per_container --executor-memory $memory_per_container --executor-cores $cores_per_container
}

# Show the web ui URL
function show_master_webui() {
    echo "http://$MASTER:8080"
}

# Stop the Spark slaves
function reset_environment() {
    num_nodes=$1
    stop_master
    stop_slaves
    sleep 2
    start_master
    sleep 1
    if [ ${num_nodes} -eq 1 ] ; then
        echo "Start Spark cluster locally"
        start_slaves_locally $*
    else
        echo "Start Spark cluster globally"
        start_slaves $*
    fi
    sleep 3
}
