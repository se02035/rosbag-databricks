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