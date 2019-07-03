# Rosbag in Databricks
This repository provides instructions and sample code for reading rosbag files (http://wiki.ros.org/rosbag) in Azure Databricks. It is based on the RosbagInputFormat from https://github.com/valtech/ros_hadoop.


Rosbag (http://wiki.ros.org/rosbag)
## Rosbag Data Structure
http://wiki.ros.org/Bags/Format
## RosbagInputFormat

# Sample Data


Header Message

(904392,
{u'data': {u'callerid': bytearray(b'/can_node'),
    u'latching': bytearray(b'0'),
    u'md5sum': bytearray(b'33747cb98e223cafb806d7e94cb4071f'),
    u'message_definition': bytearray(b"Header header\n\nCanMessage msg\n\n================================================================================\nMSG: std_msgs/Header\n# Standard metadata for higher-level stamped data types.\n# This is generally used to communicate timestamped data \n# in a particular coordinate frame.\n# \n# sequence ID: consecutively increasing ID \nuint32 seq\n#Two-integer timestamp that is expressed as:\n# * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called \'secs\')\n# * stamp.nsec: nanoseconds since stamp_secs (in Python the variable is called \'nsecs\')\n# time-handling sugar is provided by the client library\ntime stamp\n#Frame this data is associated with\n# 0: no frame\n# 1: global frame\nstring frame_id\n\n================================================================================\nMSG: dataspeed_can_msgs/CanMessage\nuint8[8] data\nuint32 id\nbool extended\nuint8 dlc\n"),
    u'topic': bytearray(b'/can_bus_dbw/can_rx'),
    u'type': bytearray(b'dataspeed_can_msgs/CanMessageStamped')
    },
   u'header': {u'conn': 0,
    u'op': 7,
    u'topic': bytearray(b'/can_bus_dbw/can_rx')
    }
})


Data Message

(904393,
{u'data': bytearray(b'<\x1a\x00\x008-\tX@=\x0b\x0b\x00\x00\x00\x00\xe7\xff\x14\x00\xe9\x03\x00\x00k\x00\x00\x00\x00\x06'),
   u'header': {u'conn': 0, u'op': 2, u'time': 804750312946085176}
})


Kudos
https://github.com/jr-robotics/ros-message-parser-net
