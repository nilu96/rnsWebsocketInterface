# rnsWebsocketInterface

Custom WebSocket client and server interfaces for the Reticulum Network Stack
(RNS).

The repository contains two Reticulum interface modules:

- `WebSocketServerInterface.py` listens for WebSocket clients and creates one
  spawned Reticulum interface per accepted connection.
- `WebSocketClientInterface.py` connects to a WebSocket server and carries RNS
  packets over that connection.

Each binary WebSocket message is treated as one Reticulum packet. Text messages
are ignored.

## Requirements

Install the Python dependencies:

```sh
python3 -m pip install -r requirements.txt
```

## Installation

Copy both interface files into the `interfaces` directory of the Reticulum
configuration you want to use:

```sh
mkdir -p ~/.reticulum/interfaces
cp src/WebSocketServerInterface.py ~/.reticulum/interfaces/
cp src/WebSocketClientInterface.py ~/.reticulum/interfaces/
```

The server interface loads `WebSocketClientInterface.py` from the same
Reticulum `interfaces` directory when it accepts peers, so both files must be
present even on a server-only node.

## Configuration

Add one of the following interface entries to the `[interfaces]` section of
your Reticulum configuration.

### Server

```ini
[[WebSocket Server]]
  type = WebSocketServerInterface
  enabled = true
  name = WebSocket Server Interface
  mode = gateway
  bind_ip = 0.0.0.0
  bind_port = 45236
  bitrate = 10000000
```

### Client

```ini
[[WebSocket Client]]
  type = WebSocketClientInterface
  enabled = true
  name = WebSocket Client Interface
  mode = gateway
  target_host = 127.0.0.1
  target_port = 45236
  bitrate = 10000000
```

Start Reticulum normally after updating the configuration:

```sh
rnsd
```

## Manual Test Setup

The `test/` directory contains separate Reticulum configurations for a local
server and client. The scripts copy the current interface files into those test
configurations before starting Reticulum.

In two terminals, run:

```sh
cd test
python3 test.py
```

```sh
cd test
python3 test-client.py
```

The server listens on `127.0.0.1:45236`, and the client connects to that same
address.

## Notes

- The server interface is a listener and does not transmit packets directly.
- Accepted peers are added as spawned `WebSocketClientInterface` instances.
- Initiating clients automatically reconnect after a dropped connection.
- The default virtual bitrate is `10_000_000` bps.
- The hardware MTU reported to Reticulum is `1200` bytes.
