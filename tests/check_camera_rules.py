#!/usr/bin/env python3
"""Check source ownership in shipped templates and rosters without starting them."""

import json
from pathlib import Path
import re


def check_template(document):
    command = document["command"]
    name = document["name"]
    assert command[0] == "gst-launch-1.0", (name, "templates run a single encoder")
    inputs = re.findall(r"\b(?:v4l2src|fdsrc|nvarguscamerasrc|libcamerasrc)\b", " ".join(command))
    assert len(inputs) == 1, (name, "exactly one input owner")
    devices = [arg for arg in command if arg.startswith("device=")]
    if document["input"] == "device":
        assert command.count("v4l2src") == 1 and devices == ["device=${device}"], name
    else:
        assert document["input"] == "stdin" and command.count("fdsrc") == 1, name
        assert "fd=0" in command and "rawvideoparse" in command and not devices, name
    assert command.count("rtph264pay") == 1 and command.count("udpsink") == 1, name
    assert command.count("tee") == 1, (name, "snapshot and RTP share the same input")
    assert command.count("jpegenc") == 1 and document["snapshot"], name


def check(root):
    templates = {}
    for path in sorted((root / "pipelines").glob("*.json")):
        document = json.loads(path.read_text())
        check_template(document)
        templates[document["name"]] = document
    assert len(templates) == 9, "three encoders with three source inputs each"
    for path in sorted((root / "examples").glob("*.json")):
        document = json.loads(path.read_text())
        devices = set()
        for source in document.get("sources", []):
            if path.name == "pipeline-sources.json":
                assert source.get("kind") == "pipeline", (path, "native and simulated examples use pipelines")
            if source.get("kind") != "pipeline":
                continue
            pipeline = source["pipeline"]
            template = templates[pipeline["template"]]
            if template["input"] == "device":
                assert not pipeline.get("producer"), (path, "a device has no second producer")
                device = pipeline["parameters"]["device"]
                assert device not in devices, (path, "a device cannot have two owners")
                devices.add(device)
            else:
                producer = pipeline["producer"]
                assert Path(producer[0]).name == "xgc2_ros_image_pipe", (path, "ROS topics feed fdsrc")
                assert "--topic" in producer and not any("/dev/" in arg for arg in producer), path
                assert "device" not in pipeline.get("parameters", {}), path
    controller = (root / "internal/mediaedge/pipeline_source.go").read_text()
    constructor = controller.split("func newPipelineControl", 1)[1].split("func (control *pipelineControl)", 1)[0]
    assert "state: pipelineIdle" in constructor and "startPipelineRun(" not in constructor
    assert controller.count("startPipelineRun(control.spec,") == 1, "only start opens the source"
    assert "process.command.Wait()" in controller and "run.terminate(control.stopGrace)" in controller
    lease = (root / "internal/mediaedge/mediamtx_server.go").read_text()
    assert "source.pending++" in lease and 'Operation: "start"' in lease
    assert "!source.hasConsumersLocked()" in lease and 'Operation: "stop"' in lease
    tests = (root / "internal/mediaedge/pipeline_server_test.go").read_text()
    assert "TestMediaEdgeRunsAPipelineOnlyWhileItIsWatched" in tests
    assert "TestMediaEdgeStopsTheWholePipelineWhenItCloses" in tests


if __name__ == "__main__":
    check(Path(__file__).resolve().parents[1])
    print("Native camera template and lease checks passed")
