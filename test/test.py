import RNS

import time
import os
import shutil


configdir = os.path.join(os.path.dirname(__file__), "rns_config_server")

# copy src/WebSocketServerInterface.py and src/WebSocketClientInterface.py to configdir/interfaces if they don't exist
interfaces_dir = os.path.join(configdir, "interfaces")
if not os.path.exists(interfaces_dir):
    os.makedirs(interfaces_dir)
shutil.copy("../src/WebSocketServerInterface.py", interfaces_dir)
shutil.copy("../src/WebSocketClientInterface.py", interfaces_dir)


reticulum = RNS.Reticulum(configdir=configdir)

if reticulum.is_connected_to_shared_instance:
    RNS.log("Started rns connected to another shared local instance, this is probably NOT what you want!", RNS.LOG_WARNING)
else:
    RNS.log("Started rns", RNS.LOG_NOTICE)

time.sleep(1)

identity = RNS.Identity()

destination = RNS.Destination(
    identity,
    RNS.Destination.IN,
    RNS.Destination.SINGLE,
    "testdestination" + str(time.time_ns())
)

try:
    while True:
        destination.announce()
        time.sleep(1)
except KeyboardInterrupt:
    RNS.log("Shutting down rns", RNS.LOG_NOTICE)
    exit()