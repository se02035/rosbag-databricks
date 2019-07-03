# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Introduction
# MAGIC This is a walk-through on how to install and use ROS tools on Azure Databricks (ADX). Additionally, it provides some additional information on issues encountered and workarounds we applied .
# MAGIC 
# MAGIC In a nutshell, these are the steps:
# MAGIC 1. Mount an Azure Storage account
# MAGIC 2. Install the ROS tools & libraries on ADX
# MAGIC 3. Test the setup
# MAGIC 
# MAGIC > **Attention:** 
# MAGIC > 
# MAGIC *Since ROS (https://www.ros.org/) officially only supports Python 2 (2.7) ensure the provisioned Azure Databricks (ADX) cluster runs Python 2, as well.*
# MAGIC 
# MAGIC **Additional resources**
# MAGIC - Official ROS page: https://www.ros.org/
# MAGIC - ROS Bags - format & specification: http://wiki.ros.org/Bags and http://wiki.ros.org/Bags/Format/2.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Prerequisites
# MAGIC Although not mandatory, it is recommended to mount an Azure Storage account to ADX. Alternativly, you can also copy all the required resources (ROS bag files, etc) to the ADX cluster when needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Create SPN and assign RBAC
# MAGIC 
# MAGIC Ensure there is a SPN that has proper rights to access the relevant Azure Storage account. See https://docs.microsoft.com/en-us/azure/storage/common/storage-auth-aad-rbac-portal?toc=%2fazure%2fstorage%2fblobs%2ftoc.json

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Connect Databricks (DBX) secret scope with Azure KeyVault instance
# MAGIC 
# MAGIC Use KeyVault to store secrets/credentials. Follow introductions here https://docs.azuredatabricks.net/user-guide/secrets/secret-scopes.html#create-an-azure-key-vault-backed-secret-scope

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Mount a Azure Blob Storage account (ADLS gen2)
# MAGIC 
# MAGIC Create and mount an Azure Blob Storagae account (ADLS gen2) that will contain the data (ROS bag files, scripts, libraries, etc) which needs to be available on ADX

# COMMAND ----------

# Azure service principal
SPN_CLIENT_ID ='xxxxxxxxxxxxxxxxxx'
SPN_TENANT_ID = 'xxxxxxxxxxxxxxxxxxxxx'
SECRETS_SPN_SCOPE = 'key-vault-secrets'
SECRETS_SPN_KEY = 'db-spn'

# Azure storage endpoint & mount specifics
ADLS_NAME = 'oliadls'
ADLS_FILESYSTEM = 'data'
ADLS_MOUNT_SOURCE = 'abfss://' + ADLS_FILESYSTEM + '@' + ADLS_NAME + '.dfs.core.windows.net/'
ADLS_MOUNT_DESTINATION = '/mnt/rosdata'

# COMMAND ----------

configs = {'fs.azure.account.auth.type': 'OAuth',
           'fs.azure.account.oauth.provider.type': 'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider',
           'fs.azure.account.oauth2.client.id': SPN_CLIENT_ID,
           'fs.azure.account.oauth2.client.secret': dbutils.secrets.get(scope = SECRETS_SPN_SCOPE, key = SECRETS_SPN_KEY),
           'fs.azure.account.oauth2.client.endpoint': 'https://login.microsoftonline.com/' + SPN_TENANT_ID + '/oauth2/token',
           'fs.azure.createRemoteFileSystemDuringInitialization': 'true' }

dbutils.fs.mount(
  source = ADLS_MOUNT_SOURCE,
  mount_point = ADLS_MOUNT_DESTINATION,
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Setup cluster initialization
# MAGIC 
# MAGIC To simplify the provisioning of the ADX cluster we'll use an initialization script. This script will be executed on all nodes whenever the cluster will be started

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 2.1 Create the cluster's init script and upload it

# COMMAND ----------

# MAGIC %md
# MAGIC The bash script will install the ROS base components using the recommended / official installation process - apt-get (described on ros.org). In a standard ROS installation the user needs to execute an installed ROS bash script which updates relevant environment variables (like PYTHONPATH, etc). This process doesn't work on ADX (find the workaround below) 

# COMMAND ----------

INIT_SCRIPT_FOLDER = 'dbfs:/databricks/scripts/init/'
INIT_SCRIPT_FILENAME = 'init_spark.bash'

INIT_SCRIPT_ABSOLUTPATH = INIT_SCRIPT_FOLDER + INIT_SCRIPT_FILENAME

# COMMAND ----------

# create a new folder on the cluster for the init script
dbutils.fs.mkdirs(INIT_SCRIPT_FOLDER)

# COMMAND ----------

# Init script
# upload the script to the cluster

dbutils.fs.put(INIT_SCRIPT_ABSOLUTPATH,"""
#!/bin/bash

sudo sh -c 'echo "deb http://packages.ros.org/ros/ubuntu $(lsb_release -sc) main" > /etc/apt/sources.list.d/ros-latest.list'
sudo apt-key adv --keyserver 'hkp://keyserver.ubuntu.com:80' --recv-key C1CF6E31E6BADE8868B172B4F42ED6FBAB17C654

sudo apt-get update -y

# install the ROS base (contains libraries, non-GUI tools)
sudo apt-get install ros-kinetic-ros-base -y
sudo rosdep init
rosdep update

sudo echo "source /opt/ros/kinetic/setup.bash" >> /etc/profile
source /etc/profile

# ensure the ROS environment variables will be transported to the worker nodes
# setting the PYTHONPATH, etc doesn't work - will be overridden.
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
display(dbutils.fs.ls(INIT_SCRIPT_ABSOLUTPATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Configure the cluster to run the script
# MAGIC 
# MAGIC Follow this tutorial and use the UI to configure the init script: https://docs.azuredatabricks.net/user-guide/clusters/init-scripts.html#config-cluster-scoped. Once done, restart the Databricks cluster to run the init script

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Update environment variables and module lookup paths
# MAGIC 
# MAGIC Since we installed the ROS artifacts (tools, librarires, etc) using apt-get and didn't use the Databricks tools to upload/install a library, python package, we have to update the lookup paths so that we can access the ROS artifacts. 
# MAGIC 
# MAGIC **ATTENTION**: Until Datbricks Runtime 5.2 it was enough to invoke the function '*update_ros_environment' just once at the beginning (and the cluster saved the updates in it's internal state). But this changed and in version above 5.2 the function 'update_ros_environment' has to be invoked in every cell where access to ROS libraries is required 
# MAGIC 
# MAGIC As an alternative the ROS Python libraries can be re-packaged and uploaded/installed on the Databricks cluster. In this case the lookup paths are correctly configured (no need to update PYTHONPATH nor sys.path).

# COMMAND ----------

def update_ros_environment():
  import os
  import sys
  
  ROS_DIST = 'kinectic'
  
  missing_ros_module_path_0 = '/opt/ros/' + ROS_DIST + '/lib/python2.7/dist-packages'
  missing_ros_module_path_1 = '/usr/lib/python2.7/dist-packages'

  # PYTHONPATH will not be correctly set on the driver. 
  # The PYTHONPATH needs to be updated if you need to run a bash command that
  # references the ROS python modules (includes the ROS tools like rosbag, rostopic, etc)
  if missing_ros_module_path_0 not in os.environ['PYTHONPATH']:
    os.environ['PYTHONPATH'] = os.environ['PYTHONPATH'] + ':' + missing_ros_module_path_0  + ':' + missing_ros_module_path_1

  # Update sys.path to be able to use the ROS python modules in the Databricks notebook (pyspark)
  if missing_ros_module_path_0 not in sys.path:
    sys.path.insert(0,missing_ros_module_path_0)
    sys.path.insert(0,missing_ros_module_path_1)
    
    
update_ros_environment()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Examples
# MAGIC 
# MAGIC ### 4.1 Using the ROS Python modules
# MAGIC 
# MAGIC Load the ROS Python libaries and ensure that the lookup paths are correct

# COMMAND ----------

#!/usr/bin/env python
import rospy
import cv_bridge 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Use the ROS tools (bash) - if avaiable

# COMMAND ----------

# MAGIC %sh
# MAGIC # sample ROS bag file: /dbfs/databricks/samples/rosbag/sample_moriyama_150324.bag
# MAGIC rosbag info /dbfs/databricks/samples/rosbag/sample_moriyama_150324.bag