# To use the interface place it in the folder
# ~/.reticulum/interfaces, and add an interface entry to
# your Reticulum configuration file similar to this:

#  [[Example Websocket Client Interface]]
#    type = WebSocketClientInterface
#    enabled = no
#    mode = gateway
#    target_host = 127.0.0.1
#    target_port = 43245
#    bitrate = 1_000_000

from time import sleep
import threading
import asyncio

import RNS
from RNS.Interfaces.Interface import Interface


import importlib
if importlib.util.find_spec('websockets') != None:
    import websockets
else:
    RNS.log("Using this interface requires the 'websockets' module to be installed.", RNS.LOG_CRITICAL)
    RNS.log("You can install it with the command: python3 -m pip install websockets", RNS.LOG_CRITICAL)
    RNS.panic()


# Let's define our custom interface class. It must
# be a sub-class of the RNS "Interface" class.
class WebSocketClientInterface(Interface):
    # All interface classes must define a default
    # IFAC size, used in IFAC setup when the user
    # has not specified a custom IFAC size. This
    # option is specified in bytes.
    DEFAULT_IFAC_SIZE = 16

    BITRATE_GUESS = 10_000_000
    RECONNECT_WAIT = 5
    RECONNECT_MAX_TRIES = None
    INITIAL_CONNECT_TIMEOUT = 5
    SYNCHRONOUS_START = True

    # The following properties are local to this
    # particular interface implementation.
    owner          = None
    target_host    = None
    target_port    = None
    bitrate        = None

    # All Reticulum interfaces must have an __init__
    # method that takes 2 positional arguments:
    # The owner RNS Transport instance, and a dict
    # of configuration values.
    def __init__(self, owner, configuration, connected_websocket=None, **kwargs):
        # We start out by initialising the super-class
        super().__init__()

        # To make sure the configuration data is in the
        # correct format, we parse it through the following
        # method on the generic Interface class. This step
        # is required to ensure compatibility on all the
        # platforms that Reticulum supports.
        ifconf = Interface.get_config_obj(configuration)

        # Read the interface name from the configuration
        # and set it on our interface instance.
        name = ifconf.get("name", None)
        if name == None:
            if kwargs.get("name", None) is not None:
                name = kwargs.get("name")
            else:
                raise ValueError(f"No name specified for {self}")
        self.name = name

        # We read configuration parameters from the supplied
        # configuration data, and provide default values in
        # case any are missing.
        target_host = ifconf.get("target_host", None)
        target_port = ifconf.get("target_port", None)
        bitrate     = ifconf.get("bitrate", self.BITRATE_GUESS)
        

        # In case no port is specified, we abort setup by
        # raising an exception.
        if target_port == None:
            if kwargs.get("target_port", None) is not None:
                target_port = kwargs.get("target_port")
            else:
                raise ValueError(f"No target port specified for {self}")

        # In case no host is specified, we abort setup by
        # raising an exception.
        if target_host == None:
            if kwargs.get("target_host", None) is not None:
                target_host = kwargs.get("target_host")
            else:
                raise ValueError(f"No target host specified for {self}")    

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
        self.owner       = owner
        self.target_host = target_host
        self.target_port = target_port
        self.bitrate     = bitrate

        self.detached = False
        self.initiator = connected_websocket is None
        self.reconnecting = False
        self.never_connected = True
        self.websocket = connected_websocket
        self.loop = None
        self._stop_event = None
        self._thread = None

        if connected_websocket is not None:
            self.online = True
            self.never_connected = False
        else:
            self.initial_connect()

    def initial_connect(self):
        self._thread = threading.Thread(target=self._run_initiator_loop, daemon=True)
        self._thread.start()

    def _run_initiator_loop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._stop_event = asyncio.Event()
        try:
            self.loop.run_until_complete(self._connect_loop())
        finally:
            self.loop.close()

    async def _connect_loop(self):
        attempts = 0
        while not self.detached:
            try:
                RNS.log("Establishing WebSocket connection for "+str(self)+"...", RNS.LOG_DEBUG)
                async with websockets.connect(
                    "ws://" + self.target_host + ":" + str(self.target_port),
                    open_timeout=self.INITIAL_CONNECT_TIMEOUT,
                    max_size=None,
                ) as websocket:
                    self.websocket = websocket
                    self.online = True
                    self.never_connected = False
                    attempts = 0
                    RNS.log("WebSocket connection for "+str(self)+" established", RNS.LOG_DEBUG)
                    await self._read_loop()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.online = False
                self.websocket = None
                if self.detached:
                    break
                attempts += 1
                if self.never_connected:
                    RNS.log("Initial connection for "+str(self)+" could not be established: "+str(e), RNS.LOG_ERROR)
                else:
                    RNS.log("Connection attempt for "+str(self)+" failed: "+str(e), RNS.LOG_DEBUG)

                if self.RECONNECT_MAX_TRIES is not None and attempts > self.RECONNECT_MAX_TRIES:
                    RNS.log("Max reconnection attempts reached for "+str(self), RNS.LOG_ERROR)
                    self.teardown()
                    break

                await asyncio.sleep(self.RECONNECT_WAIT)
    
    def process_incoming(self, data):
        if self.online and not self.detached:
            self.rxb += len(data)
            if self.parent_interface is not None:
                self.parent_interface.rxb += len(data)
            self.owner.inbound(data, self)

    def _mark_sent(self, data):
        self.txb += len(data)
        if self.parent_interface is not None:
            self.parent_interface.txb += len(data)

    async def _read_loop(self):
        try:
            async for message in self.websocket:
                if isinstance(message, bytes) and len(message) > RNS.Reticulum.HEADER_MINSIZE:
                    self.process_incoming(message)
                elif not isinstance(message, bytes):
                    RNS.log("Ignoring non-binary WebSocket message on "+str(self), RNS.LOG_DEBUG)
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            RNS.log("An interface error occurred for "+str(self)+", the contained exception was: "+str(e), RNS.LOG_WARNING)
        finally:
            self.online = False
            self.websocket = None
            if self.initiator and not self.detached:
                RNS.log("The WebSocket for "+str(self)+" was closed, attempting to reconnect...", RNS.LOG_WARNING)
            elif not self.detached:
                RNS.log("The WebSocket for remote client "+str(self)+" was closed.", RNS.LOG_DEBUG)
                self.teardown()

    def process_outgoing(self, data):
        if self.online and not self.detached and self.websocket is not None:
            try:
                self.writing = True
                if self.loop is None:
                    self.loop = asyncio.get_running_loop()
                future = asyncio.run_coroutine_threadsafe(self._send(bytes(data)), self.loop)
                future.add_done_callback(self._send_done)

            except Exception as e:
                self.writing = False
                RNS.log("Exception occurred while transmitting via "+str(self)+", tearing down interface", RNS.LOG_ERROR)
                RNS.log("The contained exception was: "+str(e), RNS.LOG_ERROR)
                self.teardown()

    async def _send(self, data):
        await self.websocket.send(data)
        self._mark_sent(data)

    def _send_done(self, future):
        self.writing = False
        try:
            future.result()
        except Exception as e:
            RNS.log("Exception occurred while transmitting via "+str(self)+", tearing down interface", RNS.LOG_ERROR)
            RNS.log("The contained exception was: "+str(e), RNS.LOG_ERROR)
            self.teardown()

    def detach(self):
        self.detached = True
        self.online = False
        if self.loop is not None and not self.loop.is_closed():
            async def close():
                if self.websocket is not None:
                    await self.websocket.close()

            asyncio.run_coroutine_threadsafe(close(), self.loop)

    def teardown(self):
        if self.initiator and not self.detached:
            RNS.log("The interface "+str(self)+" experienced an unrecoverable error and is being torn down. Restart Reticulum to attempt to open this interface again.", RNS.LOG_ERROR)
            if RNS.Reticulum.panic_on_interface_error:
                RNS.panic()
        else:
            RNS.log("The interface "+str(self)+" is being torn down.", RNS.LOG_VERBOSE)

        self.online = False
        self.OUT = False
        self.IN = False
        self.detached = True

        if self.parent_interface is not None:
            while self in self.parent_interface.spawned_interfaces:
                self.parent_interface.spawned_interfaces.remove(self)

        if not self.initiator:
            RNS.Transport.remove_interface(self)

    def __str__(self):
        ip_str = f"{self.target_host}"

        return "WebSocketInterface["+str(self.name)+"/"+ip_str+":"+str(self.target_port)+"]"

# Finally, register the defined interface class as the
# target class for Reticulum to use as an interface
interface_class = WebSocketClientInterface
