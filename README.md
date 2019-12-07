# spark-kit: Toolkits simplifying the experiments on Spark

Typically, Spark runs in YARN, which is not convenient if we need finer control of executor placement (for example, run in a single machine with specific number of executors with exactly configuration).
Standalone better suites this use cases. 

# Example

1. In order to use the spark-kit:
```bash
git clone https://github.com/stevenybw/spark-kit
cd spark-kit
source manage-standalone.sh
```
2. Get Spark official release
```bash
wget https://www.apache.org/dyn/closer.lua/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
```
3. Check the environment and follow the direction
```bash
check_environment
```
4. Adjust the parameters in `manage-standalone.sh`.
5. Establish Spark standalone cluster with all the nodes in ${SLAVES_HOSTLIST}
```bash
reset_environment $DIST
```
6. Establish Spark standalone cluster with a single node (the first node in ${SLAVES_HOSTLIST})
```bash
reset_environment $LOCAL
```
7. Establish Spark standalone cluster with a single node (current node running the script)
```bash
reset_environment_locally $LOCAL
```
8. Check the Spark standalone resource manager master
```bash
show_master_webui
```
9. Show the command to launch a spark shell (its argument must be the same as how you setup the environments, assume distributed here)
```bash
show_spark_shell_command $DIST
```
10. Or launch a spark shell
```bash
enter_spark_shell $DIST
```
11. See the session web UI of the spark job at port 4040