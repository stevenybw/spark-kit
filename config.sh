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

# ===============================================================================

##### The following is specific to Sparker ######

# Mapping from config name to extra Spark flags
function getSparkFlags() {
    CONFIG=$1
    if [ "$CONFIG" = "tree" ]; then
        echo "$BASE_SPARK_FLAGS"
    elif [ "$CONFIG" = "spag" ]; then
        echo "$BASE_SPARK_FLAGS --conf spark.spag.enableAll=true --conf spark.spag.kind=rdd_imm_sc"
    elif [ "$CONFIG" = "spag_rdd" ]; then
        echo "$BASE_SPARK_FLAGS --conf spark.spag.enableAll=true --conf spark.spag.kind=rdd"
    elif [ "$CONFIG" = "spag_rdd_imm" ]; then
        echo "$BASE_SPARK_FLAGS --conf spark.spag.enableAll=true --conf spark.spag.kind=rdd_imm"
    else
        echo Unrecognized config $CONFIG
        exit 1
    fi
}

# Controls where the output log file will be written to
RESULT_PREFIX=""

# Config must be in {tree, spag}
function getResultDir() {
    CONFIG=$1
    if [ "$CONFIG" = "tree" ]; then
        echo "./${RESULT_PREFIX}tree"
    elif [ "$CONFIG" = "spag" ]; then
        echo "./${RESULT_PREFIX}spag"
    elif [ "$CONFIG" = "spag_rdd" ]; then
        echo "./${RESULT_PREFIX}spag_rdd"
    elif [ "$CONFIG" = "spag_rdd_imm" ]; then
        echo "./${RESULT_PREFIX}spag_rdd_imm"
    else
        echo Unrecognized config $CONFIG
        exit 1
    fi
}