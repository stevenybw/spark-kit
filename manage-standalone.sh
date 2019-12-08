#!/bin/bash
# Scripts to manage Spark standalone daemons
# Bowen Yu <stevenybw@hotmail.com>

set -u

# Directory to the bash

function get_bash_source_absolute() {
    ORIGINAL_PWD=$(pwd)
    RELATIVE_SCRIPT_DIR=$(dirname $BASH_SOURCE)
    cd $RELATIVE_SCRIPT_DIR
    echo $(pwd)
    cd $ORIGINAL_PWD
}

SCRIPT_DIR=$(get_bash_source_absolute)

source ${SCRIPT_DIR}/config.sh

# Several typical configurations

# Run locally: 1 node, 2 numa-node, 6 executors, (8 cores + 30 GB) for each executor
LOCAL="1 2 6 8 30g"

# Run distributed: 8 nodes, 2 numa-node, 6 executors, (8 cores + 30 GB) for each executor
DIST="8 2 6 8 30g"

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

# Start the Spark slaves (select the first N nodes in SLAVES_HOSTLIST)
function start_slaves() {
    num_nodes=$1
    numa_nodes_per_node=$2
    container_per_node=$3
    cores_per_container=$4
    memory_per_container=$5
    
    hostlist=${SLAVES_HOSTLIST[@]:0:$num_nodes}
    remote_cmd="SPARK_HOME=${SPARK_HOME} SPARK_WORKER_INSTANCES=${container_per_node} SPARK_NUMA_NODES=${numa_nodes_per_node} ${SCRIPT_DIR}/sbin/start-slave-numabind.sh -c ${cores_per_container} -m ${memory_per_container} -i "'$(hostname -f)'" -d ${SPARK_WORK_DIR} spark://${MASTER}:7077"
    clush -w $(echo $hostlist | sed "s/ /,/g") ${remote_cmd}
}

# Start the Spark slaves locally
function start_slaves_locally() {
    num_nodes=$1
    numa_nodes_per_node=$2
    container_per_node=$3
    cores_per_container=$4
    memory_per_container=$5
    
    hostlist=$(hostname)
    remote_cmd="SPARK_HOME=${SPARK_HOME} SPARK_WORKER_INSTANCES=${container_per_node} SPARK_NUMA_NODES=${numa_nodes_per_node} ${SPARK_HOME}/sbin/start-slave-numabind.sh -c ${cores_per_container} -m ${memory_per_container} -i "'$(hostname -f)'" -d ${SPARK_WORK_DIR} spark://${MASTER}:7077"
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

# Reset the environment with the hosts in AVAILABLE_HOSTLIST as slaves
# Example: reset_environment 8 2 6 4 30g
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

# Reset the environment with current host as slave (single-node)
# Example: reset_environment_locally 8 2 6 4 30g
function reset_environment_locally() {
    num_nodes=$1
    if [ ! ${num_nodes} -eq 1 ] ; then
        echo "Invalid argument"
        return
    fi
    stop_master
    stop_slaves
    sleep 2
    start_master
    sleep 1
    if [ ${num_nodes} -eq 1 ] ; then
        echo "Start Spark cluster locally"
        start_slaves_locally $*
    fi
    sleep 3
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

# Enter the Spark shell
function enter_spark_shell() {
    echo $(show_spark_shell_command $*)
    eval $(show_spark_shell_command $*)
}

# Show the web ui URL
function show_master_webui() {
    echo "http://$MASTER:8080"
}

# Show the session web ui URL
function show_session_webui() {
    echo "http://$MASTER:4040"
}
