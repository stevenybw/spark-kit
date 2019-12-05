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
5. Establish Spark standalone cluster with 8 nodes
```bash
reset_environment $DIST
```
6. Establish Spark standalone cluster with a single node
```bash
reset_environment $LOCAL
```
7. Show the command to launch a spark shell
```bash
show_spark_shell_command
```