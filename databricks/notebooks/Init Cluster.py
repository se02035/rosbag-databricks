# Databricks notebook source
# MAGIC %md
# MAGIC #1. Setup cluster initialization

# COMMAND ----------

init_script_folder = "dbfs:/databricks/scripts/init/"
init_script_filename = "init_spark.bash"

init_script_absolutpath = init_script_folder + init_script_filename

# COMMAND ----------

# create a new folder on the cluster for the init script
dbutils.fs.mkdirs(init_script_folder)

# COMMAND ----------

# Init script
# upload the script to the cluster

dbutils.fs.put(init_script_absolutpath,"""
#!/bin/bash
sudo sh -c 'echo "deb http://packages.ros.org/ros/ubuntu $(lsb_release -sc) main" > /etc/apt/sources.list.d/ros-latest.list'
sudo apt-key adv --keyserver 'hkp://keyserver.ubuntu.com:80' --recv-key C1CF6E31E6BADE8868B172B4F42ED6FBAB17C654

sudo apt-get update -y

sudo apt-get install ros-kinetic-ros-base -y
sudo rosdep init
rosdep update

sudo echo "source /opt/ros/kinetic/setup.bash" >> /etc/profile
source /etc/profile

sudo echo ROS_ROOT=$ROS_ROOT >> /etc/environment
sudo echo ROS_PACKAGE_PATH=$ROS_PACKAGE_PATH >> /etc/environment
sudo echo ROS_MASTER_URI=$ROS_MASTER_URI >> /etc/environment
sudo echo ROS_VERSION=$ROS_VERSION >> /etc/environment
sudo echo ROSLISP_PACKAGE_DIRECTORIES=$ROSLISP_PACKAGE_DIRECTORIES >> /etc/environment
sudo echo ROS_DISTRO=$ROS_DISTRO >> /etc/environment
sudo echo ROS_ETC_DIR=$ROS_ETC_DIR >> /etc/environment

apt-get update && apt-get install -y --no-install-recommends \
  openjdk-8-jdk-headless \
  && rm -rf /var/lib/apt/lists/*

python2 -m pip install --upgrade pip
""", True)

# COMMAND ----------

# verify the file exists on the cluster
display(dbutils.fs.ls(init_script_absolutpath))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configure the cluster to run the script

# COMMAND ----------

# MAGIC %md
# MAGIC Follow this tutorial and use the UI to configure the init script. https://docs.azuredatabricks.net/user-guide/clusters/init-scripts.html#config-cluster-scoped

# COMMAND ----------

# MAGIC %md
# MAGIC Start the Databricks cluster to run the init script

# COMMAND ----------

# MAGIC %md 
# MAGIC #2. Update environment variables and module lookup paths

# COMMAND ----------

import os
import sys

missing_ros_module_path = '/opt/ros/kinetic/lib/python2.7/dist-packages'

# PYTHONPATH will not be correctly set on the driver. 
# The PYTHONPATH needs to be updated if you need to run a bash command that
# references the ROS python modules (includes the ROS tools like rosbag, rostopic, etc)
os.environ['PYTHONPATH'] = os.environ['PYTHONPATH'] + ':' + missing_ros_module_path

# Update sys.path to be able to use the ROS python modules in the Databricks notebook (pyspark)
sys.path.insert(0,missing_ros_module_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Examples

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Using the ROS Python modules (pyspark)

# COMMAND ----------

#!/usr/bin/env python
import rospy

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Use the ROS tools (bash) - if avaiable

# COMMAND ----------

# MAGIC %%bash
# MAGIC rosbag --help

# COMMAND ----------

# MAGIC %%bash
# MAGIC rostopic --help
