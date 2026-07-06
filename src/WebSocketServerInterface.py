# To use the interface place it in the folder
# ~/.reticulum/interfaces, and add an interface entry to
# your Reticulum configuration file similar to this:

#  [[Example Websocket Server Interface]]
#    type = WebSocketServerInterface
#    enabled = no
#    mode = gateway
#    bind_ip = 127.0.0.1
#    bind_port = 43245
#    bitrate = 1_000_000

from time import sleep
import threading
import asyncio
import time
import os

import RNS
from RNS.Interfaces.Interface import Interface


import importlib
if importlib.util.find_spec('websockets') != None:
    import websockets
else:
    RNS.log("Using this interface requires the 'websockets' module to be installed.", RNS.LOG_CRITICAL)
    RNS.log("You can install it with the command: python3 -m pip install websockets", RNS.LOG_CRITICAL)
    RNS.panic()


WebSocketClientInterface = None

def _copy_if_present(source, target, names):
    for name in names:
        if hasattr(source, name):
            setattr(target, name, getattr(source, name))


def _load_local_client_interface():
    rns_configdir = getattr(RNS.Reticulum, "configdir", None)
    rns_interfaces_dir = os.path.join(rns_configdir, "interfaces") if rns_configdir is not None else None
    if rns_interfaces_dir != None and os.path.exists(rns_interfaces_dir):
        # import the WebSocketClientInterface from the local interfaces directory
        spec = importlib.util.spec_from_file_location("WebSocketClientInterface", os.path.join(rns_interfaces_dir, "WebSocketClientInterface.py"))
        if spec is not None:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return getattr(module, "WebSocketClientInterface", None)


# Let's define our custom interface class. It must
# be a sub-class of the RNS "Interface" class.
class WebSocketServerInterface(Interface):
    # All interface classes must define a default
    # IFAC size, used in IFAC setup when the user
    # has not specified a custom IFAC size. This
    # option is specified in bytes.
    DEFAULT_IFAC_SIZE = 16

    BITRATE_GUESS = 10_000_000
    START_TIMEOUT = 5.0

    # The following properties are local to this
    # particular interface implementation.
    owner          = None
    bind_ip        = None
    bind_port      = None
    bitrate        = None

    # All Reticulum interfaces must have an __init__
    # method that takes 2 positional arguments:
    # The owner RNS Transport instance, and a dict
    # of configuration values.
    def __init__(self, owner, configuration):
        # The following lines demonstrate handling
        # potential dependencies required for the
        # interface to function correctly.
        global WebSocketClientInterface
        WebSocketClientInterface = _load_local_client_interface()
        if WebSocketClientInterface == None:
            RNS.log("Using this interface also requires WebSocketClientInterface module.", RNS.LOG_CRITICAL)
            RNS.panic()

        # We start out by initialising the super-class
        super().__init__()

        # To make sure the configuration data is in the
        # correct format, we parse it through the following
        # method on the generic Interface class. This step
        # is required to ensure compatibility on all the
        # platforms that Reticulum supports.
        ifconf    = Interface.get_config_obj(configuration)

        # Read the interface name from the configuration
        # and set it on our interface instance.
        name      = ifconf["name"]
        self.name = name

        # We read configuration parameters from the supplied
        # configuration data, and provide default values in
        # case any are missing.
        bind_ip        = ifconf.get("bind_ip", "0.0.0.0")
        bind_port      = ifconf.get("bind_port", None)
        bitrate        = ifconf.get("bitrate", self.BITRATE_GUESS)
        

        # In case no port is specified, we abort setup by
        # raising an exception.
        if bind_port == None:
            raise ValueError(f"No port specified for {self}")

        # All interfaces must supply a hardware MTU value
        # to the RNS Transport instance. This value should
        # be the maximum data packet payload size that the
        # underlying medium is capable of handling in all
        # cases without any segmentation.
        self.HW_MTU = 1200

        # We initially set the "online" property to false,
        # since the interface has not actually been fully
        # initialised and connected yet.
        self.online   = False        
        
        # Configure internal properties on the interface
        # according to the supplied configuration.
        self.owner          = owner
        self.bind_ip        = bind_ip
        self.bind_port      = bind_port
        self.bitrate        = bitrate
        self.spawned_interfaces = []

        # Since all required parameters are now configured,
        # we will try starting the WebSocket server.
        self.loop = asyncio.new_event_loop()
        self.server = None
        self._startup_error = None
        self._started = threading.Event()
        self._thread = threading.Thread(target=self._run_server, daemon=True)
        self._thread.start()

        if not self._started.wait(self.START_TIMEOUT):
            raise SystemError(f"Timed out starting WebSocket listener for interface \"{name}\"")
        if self._startup_error is not None:
            raise SystemError(f"Could not bind WebSocket listener for interface \"{name}\"") from self._startup_error

        self.online = True

    def _run_server(self):
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._start_server())
            self._started.set()
            self.loop.run_forever()
        except Exception as e:
            self._startup_error = e
            self._started.set()
        finally:
            self.loop.run_until_complete(self._shutdown())
            self.loop.close()

    async def _start_server(self):
        self.server = await websockets.serve(self.incoming_connection, self.bind_ip, self.bind_port, max_size=None)

    async def incoming_connection(self, websocket, *args):
        RNS.log("Accepting incoming WebSocket connection", RNS.LOG_VERBOSE)

        target_host, target_port = self._remote_address(websocket)

        spawned_interface = WebSocketClientInterface(
            self.owner,
            configuration={},
            name="Client on " + self.name,
            target_host=target_host,
            target_port=target_port,
            connected_websocket=websocket,
        )

        spawned_interface.OUT = self.OUT
        spawned_interface.IN = self.IN
        spawned_interface.parent_interface = self
        spawned_interface.bitrate = self.bitrate
        spawned_interface.mode = self.mode
        spawned_interface.HW_MTU = self.HW_MTU
        spawned_interface.online = True
        spawned_interface.loop = self.loop

        _copy_if_present(self, spawned_interface, [
            "ingress_control",
            "ic_max_held_announces",
            "ic_burst_hold",
            "ic_burst_freq",
            "ic_burst_freq_new",
            "ic_new_time",
            "ic_burst_penalty",
            "ic_held_release_interval",
            "egress_control",
            "ec_pr_freq",
            "ic_pr_burst_freq_new",
            "ic_pr_burst_freq",
            "ifac_size",
            "ifac_netname",
            "ifac_netkey",
            "ifac_key",
            "ifac_identity",
            "ifac_signature",
            "announce_rate_target",
            "announce_rate_grace",
            "announce_rate_penalty",
        ])

        spawned_interface.optimise_mtu()
        RNS.log("Spawned new WebSocket Interface: "+str(spawned_interface), RNS.LOG_VERBOSE)
        RNS.Transport.add_interface(spawned_interface)
        while spawned_interface in self.spawned_interfaces:
            self.spawned_interfaces.remove(spawned_interface)
        self.spawned_interfaces.append(spawned_interface)
        await spawned_interface._read_loop()

    @staticmethod
    def _remote_address(websocket):
        remote = getattr(websocket, "remote_address", None)
        if isinstance(remote, tuple) and len(remote) >= 2:
            return remote[0], remote[1]
        return "unknown", 0

    def received_announce(self, from_spawned=False):
        if from_spawned:
            self.ia_freq_deque.append(time.time())

    def sent_announce(self, from_spawned=False):
        if from_spawned:
            self.oa_freq_deque.append(time.time())

    def received_path_request(self, from_spawned=False):
        if from_spawned:
            self.ip_freq_deque.append(time.time())

    def sent_path_request(self, from_spawned=False):
        if from_spawned:
            self.op_freq_deque.append(time.time())

    def process_outgoing(self, data):
        pass

    def detach(self):
        self.detached = True
        self.online = False
        if self.loop is not None and not self.loop.is_closed():
            self.loop.call_soon_threadsafe(self.loop.stop)
        if threading.current_thread() is not self._thread:
            self._thread.join(timeout=2.0)

    def teardown(self):
        self.detach()

    async def _shutdown(self):
        for spawned_interface in list(self.spawned_interfaces):
            spawned_interface.detached = True
            if spawned_interface.websocket is not None:
                try:
                    await spawned_interface.websocket.close()
                except Exception:
                    pass
            RNS.Transport.remove_interface(spawned_interface)
        self.spawned_interfaces.clear()

        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

    def __str__(self):
        ip_str = f"{self.bind_ip}"

        return "WebSocketServerInterface["+self.name+"/"+ip_str+":"+str(self.bind_port)+"]"

# Finally, register the defined interface class as the
# target class for Reticulum to use as an interface
interface_class = WebSocketServerInterface
