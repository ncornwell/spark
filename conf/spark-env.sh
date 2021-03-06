#!/usr/bin/env bash

# This file contains environment variables required to run Spark. Copy it as
# spark-env.sh and edit that to configure Spark for your site. At a minimum,
# the following two variables should be set:
# - SCALA_HOME, to point to your Scala installation, or SCALA_LIBRARY_PATH to
#   point to the directory for Scala library JARs (if you install Scala as a
#   Debian or RPM package, these are in a separate path, often /usr/share/java)
# - MESOS_NATIVE_LIBRARY, to point to your libmesos.so if you use Mesos
#
# If using the standalone deploy mode, you can also set variables for it:
# - SPARK_MASTER_IP, to bind the master to a different IP address
# - SPARK_MASTER_PORT / SPARK_MASTER_WEBUI_PORT, to use non-default ports
# - SPARK_WORKER_CORES, to set the number of cores to use on this machine
# - SPARK_WORKER_MEMORY, to set how much memory to use (e.g. 1000m, 2g)
# - SPARK_WORKER_PORT / SPARK_WORKER_WEBUI_PORT
# - SPARK_WORKER_INSTANCES, to set the number of worker instances/processes
#   to be spawned on every slave machine


export SCALA_HOME=/opt/spark_test/scala-2.9.3
export SPARK_MEM=20g
export SPARK_WORKER_MEMORY=20g
export MASTER="spark://db1.stg:7077"
export HADOOP_HOME=/hadoop/hadoop