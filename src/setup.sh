#====================================
# ROS LIBRARIES
#====================================

#install ROS libs/packages
sudo sh -c 'echo "deb http://packages.ros.org/ros/ubuntu $(lsb_release -sc) main" > /etc/apt/sources.list.d/ros-latest.list'
sudo apt-key adv --keyserver 'hkp://keyserver.ubuntu.com:80' --recv-key C1CF6E31E6BADE8868B172B4F42ED6FBAB17C654
sudo apt update

sudo apt install ros-melodic-rospy ros-melodic-rosbag -y

# ensure ROS environment variables are automatically 
# added to your bash session every time a new shell is launched
echo "source /opt/ros/melodic/setup.bash" >> ~/.bashrc
source ~/.bashrc

# ATTENTION: in conda: don't use the global PIP (with an updated PYTHONPATH. wont work)
# also don't use the apt-get installation as it will put the rospkg outside of the virtual env
python -m pip install -U rospkg 

#====================================
# PYTEST
#====================================

#install java
sudo apt install openjdk-8-jdk-headless  openjdk-8-jre-headless -y
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre
export PATH=$PATH:$JAVA_HOME/bin

# install pytest
conda install -c anaconda pytest

#====================================
# ROSBAG DATABRICKS LIBRARY
#====================================

cd ./rosbagdatabricks
python -m pip install -U . 
