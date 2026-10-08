"""Isolated real Python camera-source Host; no ROS, station or encoder is started."""
import io
import json
import sys
from PIL import Image
from xgc2_xrpc import Runtime
from ros_image_rtp_adapter.control_socket import SourceControlServer, SourceDescription, SnapshotCapture

path, port = sys.argv[1], int(sys.argv[2])
pixels = bytes((index % 251 for index in range(16 * 16 * 3)))
image = Image.frombytes("RGB", (16, 16), pixels)
encoded = io.BytesIO()
image.save(encoded, format="JPEG", quality=90)
jpeg = encoded.getvalue()
active = False
sequence = 7
configuration = {"rtp_host": "127.0.0.1", "rtp_port": port, "bitrate": 2000000}
runtime = Runtime(blocking_workers=4, max_calls=8, max_connections=16, max_sessions=8)


def set_active(value):
    global active
    active = value


def capture(include_rgb, require_fresh):
    global sequence
    if not active:
        raise RuntimeError("capture must own an active source")
    if require_fresh:
        sequence += 1
    return SnapshotCapture(jpeg, pixels if include_rgb else b"", 123456789, "device",
                           width=16, height=16, frame_id="python_optical", frame_sequence=sequence)


def configure(value):
    if active:
        from xgc2_xrpc import Fault
        raise Fault("conflict", "source configuration requires idle")
    if set(value) - set(configuration):
        from xgc2_xrpc import Fault
        raise Fault("conflict", "unsupported fields require restart")
    configuration.update(value)


def start():
    server = SourceControlServer(path, SourceDescription("python-camera", "127.0.0.1", port,
                                16, 16, 20.0, "python_optical"), runtime=runtime,
                                on_set_active=set_active, on_snapshot=capture,
                                on_configuration=lambda: dict(configuration), on_configure=configure)
    server.start()
    print(json.dumps({"instance_id": server.service_ref["instance_id"]}), flush=True)
    return server


server = start()
try:
    for command in sys.stdin:
        if command.strip() == "restart":
            server.stop()
            server = start()
        else:
            break
finally:
    server.stop()
    runtime.close()
