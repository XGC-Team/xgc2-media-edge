package mediaedge

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const shippedTemplates = "../../pipelines"

func shippedSource(template string, parameters map[string]PipelineValue, producer []string) SourceConfig {
	return pipelineTestSource("front", "127.0.0.1:5004", template, parameters, producer)
}

func resolveShipped(t *testing.T, source SourceConfig) (SourceConfig, error) {
	t.Helper()
	absolute, err := filepath.Abs(shippedTemplates)
	if err != nil {
		t.Fatal(err)
	}
	config, err := (Config{
		ControlAddress: "127.0.0.1:0", PipelineTemplatesDir: absolute, PipelineRuntimeDir: "/run/xgc2/test/pipelines",
		Sources: []SourceConfig{source},
	}).normalized()
	if err != nil {
		return SourceConfig{}, err
	}
	return config.Sources[0], nil
}

func TestEveryShippedTemplateLoadsAndExpands(t *testing.T) {
	entries, err := os.ReadDir(shippedTemplates)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 10 {
		t.Fatalf("shipped %d templates, want 3 encoders x 3 inputs plus a ROS image template", len(entries))
	}
	for _, entry := range entries {
		name := strings.TrimSuffix(entry.Name(), ".json")
		t.Run(name, func(t *testing.T) {
			parameters := map[string]PipelineValue{}
			var producer []string
			if name == "x264-ros-image" {
				parameters["topic"] = "/sim/camera/image_raw"
			} else if strings.HasSuffix(name, "v4l2src-raw") || strings.HasSuffix(name, "v4l2src-mjpeg") {
				parameters["device"] = "/dev/video8"
			} else {
				producer = []string{"/usr/lib/xgc2-test/image-pipe", "--topic", "/d435/image_raw"}
			}
			source, err := resolveShipped(t, shippedSource(name, parameters, producer))
			if err != nil {
				t.Fatal(err)
			}
			spec := source.Pipeline.resolved
			command := strings.Join(spec.command, " ")
			if spec.command[0] != "gst-launch-1.0" || spec.snapshotDir != "/run/xgc2/test/pipelines/front" {
				t.Fatalf("spec = %+v", spec)
			}
			for _, want := range []string{
				"udpsink host=127.0.0.1 port=5004 sync=false async=false",
				"rtph264pay pt=96 config-interval=1 mtu=1200",
				"h264parse config-interval=1",
				"location=/run/xgc2/test/pipelines/front/snapshot-%05d.jpg max-files=2",
				"framerate=10/1",
			} {
				if !strings.Contains(command, want) {
					t.Errorf("%s: %q is missing from %s", name, want, command)
				}
			}
			if strings.Contains(command, "${") {
				t.Errorf("%s: unexpanded placeholder in %s", name, command)
			}
			// One IDR per second: every encoder is told the GOP in frames.
			if strings.Contains(name, "x264") && !strings.Contains(command, "key-int-max=10") ||
				strings.Contains(name, "nvv4l2") && !strings.Contains(command, "idrinterval=10") ||
				strings.Contains(name, "mpp") && !strings.Contains(command, "gop=10") {
				t.Errorf("%s: GOP of one second is not set in %s", name, command)
			}
		})
	}
}

func TestShippedROSImageTemplateValidatesParametersAndProducer(t *testing.T) {
	for name, parameters := range map[string]map[string]PipelineValue{
		"default distribution": {"topic": "/sim/camera/image_raw"},
		"melodic":              {"topic": "/robot_1/camera/image_raw", "rosDistro": "melodic"},
		"noetic":               {"topic": "/robot_1/camera/image_raw", "rosDistro": "noetic"},
		"maximum topic":        {"topic": PipelineValue("/" + strings.Repeat("a", 200))},
	} {
		t.Run(name, func(t *testing.T) {
			source, err := resolveShipped(t, shippedSource("x264-ros-image", parameters, nil))
			if err != nil {
				t.Fatal(err)
			}
			distro := "noetic"
			if given, found := parameters["rosDistro"]; found {
				distro = string(given)
			}
			if source.Pipeline.resolved.producer[0] != filepath.Join("/opt", "ros", distro, "lib", "xgc2_camera_driver", "xgc2_ros_image_pipe") ||
				!slices.Contains(source.Pipeline.resolved.command, "bitrate=2500") {
				t.Fatalf("defaults or distribution not applied: %+v", source.Pipeline.resolved)
			}
		})
	}
	for name, parameters := range map[string]map[string]PipelineValue{
		"missing topic":        {},
		"empty topic":          {"topic": ""},
		"relative topic":       {"topic": "sim/camera/image_raw"},
		"root topic":           {"topic": "/"},
		"long topic":           {"topic": PipelineValue("/" + strings.Repeat("a", 201))},
		"topic with space":     {"topic": "/sim/camera image_raw"},
		"topic with newline":   {"topic": "/sim/camera/image_raw\n"},
		"topic with element":   {"topic": "/sim/camera/image_raw!fakesink"},
		"topic with query":     {"topic": "/sim/camera/image_raw?x=1"},
		"unknown parameter":    {"topic": "/sim/camera/image_raw", "extra": "1"},
		"format override":      {"topic": "/sim/camera/image_raw", "format": "rgb"},
		"builtin override":     {"topic": "/sim/camera/image_raw", "width": "1"},
		"unknown distribution": {"topic": "/sim/camera/image_raw", "rosDistro": "foxy"},
		"distribution case":    {"topic": "/sim/camera/image_raw", "rosDistro": "Noetic"},
		"distribution path":    {"topic": "/sim/camera/image_raw", "rosDistro": "../noetic"},
		"invalid bitrate":      {"topic": "/sim/camera/image_raw", "bitrateKbps": "fast"},
		"long bitrate":         {"topic": "/sim/camera/image_raw", "bitrateKbps": "1000000"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := resolveShipped(t, shippedSource("x264-ros-image", parameters, nil)); err == nil {
				t.Fatalf("accepted %v", parameters)
			}
		})
	}
	for _, producer := range [][]string{{"/bin/true"}, {}} {
		if _, err := resolveShipped(t, shippedSource("x264-ros-image", map[string]PipelineValue{"topic": "/sim/camera/image_raw"}, producer)); err == nil ||
			!strings.Contains(err.Error(), `template "x264-ros-image" declares its own producer and refuses a source-supplied producer`) {
			t.Fatalf("source-supplied producer %q: %v", producer, err)
		}
	}
}

func TestShippedROSImageTemplateRendersBothArgumentVectors(t *testing.T) {
	source := shippedSource("x264-ros-image", map[string]PipelineValue{
		"topic": "/sim/quadrotor/front/image_raw", "rosDistro": "melodic", "bitrateKbps": "4000",
	}, nil)
	source.Width, source.Height, source.FPS = 640, 480, 30
	resolved, err := resolveShipped(t, source)
	if err != nil {
		t.Fatal(err)
	}
	spec := resolved.Pipeline.resolved
	wantProducer := []string{
		filepath.Join("/opt", "ros", "melodic", "lib", "xgc2_camera_driver", "xgc2_ros_image_pipe"), "--topic", "/sim/quadrotor/front/image_raw",
		"--format", "bgr8", "--width", "640", "--height", "480", "--max-fps", "30",
	}
	if !slices.Equal(spec.producer, wantProducer) {
		t.Fatalf("producer = %q, want %q", spec.producer, wantProducer)
	}
	wantCommand := []string{
		"gst-launch-1.0", "-q", "fdsrc", "fd=0", "blocksize=262144", "!",
		"rawvideoparse", "format=bgr", "width=640", "height=480", "framerate=30/1", "!",
		"videoconvert", "!", "tee", "name=split", "split.", "!",
		"queue", "max-size-buffers=2", "leaky=downstream", "!",
		"x264enc", "tune=zerolatency", "speed-preset=ultrafast", "key-int-max=30", "bitrate=4000", "byte-stream=true", "!",
		"video/x-h264,profile=baseline", "!", "h264parse", "config-interval=1", "!",
		"rtph264pay", "pt=96", "config-interval=1", "mtu=1200", "!",
		"udpsink", "host=127.0.0.1", "port=5004", "sync=false", "async=false", "split.", "!",
		"queue", "max-size-buffers=1", "leaky=downstream", "!", "videorate", "drop-only=true", "!",
		"video/x-raw,framerate=1/1", "!", "jpegenc", "quality=90", "!",
		"multifilesink", "location=/run/xgc2/test/pipelines/front/snapshot-%05d.jpg", "max-files=2", "post-messages=false",
	}
	if !slices.Equal(spec.command, wantCommand) {
		t.Fatalf("command = %q, want %q", spec.command, wantCommand)
	}
}

func TestTemplateInputDecidesWhetherAProducerIsRequired(t *testing.T) {
	for _, name := range []string{"x264-fdsrc", "nvv4l2-fdsrc", "mpp-fdsrc"} {
		if _, err := resolveShipped(t, shippedSource(name, nil, nil)); err == nil ||
			!strings.Contains(err.Error(), "needs a producer") {
			t.Fatalf("%s without producer: %v", name, err)
		}
	}
	if _, err := resolveShipped(t, shippedSource("x264-v4l2src-raw", map[string]PipelineValue{"device": "/dev/video0"}, []string{"/bin/true"})); err == nil ||
		!strings.Contains(err.Error(), "takes no producer") {
		t.Fatalf("device template with producer: %v", err)
	}
}

func TestTemplateOwnedProducerExpandsAndRefusesSourceProducer(t *testing.T) {
	templates, runtime := t.TempDir(), t.TempDir()
	writeTestTemplate(t, templates, testTemplate{
		name: "owned", input: "stdin",
		parameters: map[string]pipelineParameterDocument{"topic": {}},
		command:    []string{"fake-pipeline", "--size=${width}x${height}"},
		producer:   []string{"fake-producer", "--topic=${topic}", "--fps=${fps}"},
	})
	source := pipelineTestSource("cam", "127.0.0.1:5004", "owned", map[string]PipelineValue{"topic": "/sim/camera/image_raw"}, nil)
	config := Config{ControlAddress: "127.0.0.1:0", PipelineTemplatesDir: templates, PipelineRuntimeDir: runtime, Sources: []SourceConfig{source}}
	resolved, err := config.normalized()
	if err != nil {
		t.Fatal(err)
	}
	spec := resolved.Sources[0].Pipeline.resolved
	if want := []string{testBinary(t), "fake-pipeline", "--size=64x48"}; !slices.Equal(spec.command, want) {
		t.Fatalf("command = %q, want %q", spec.command, want)
	}
	if want := []string{testBinary(t), "fake-producer", "--topic=/sim/camera/image_raw", "--fps=10"}; !slices.Equal(spec.producer, want) {
		t.Fatalf("producer = %q, want %q", spec.producer, want)
	}
	for _, producer := range [][]string{{"/bin/true"}, {}} {
		config.Sources[0].Pipeline.Producer = producer
		if _, err := config.normalized(); err == nil || !strings.Contains(err.Error(), `template "owned" declares its own producer and refuses a source-supplied producer`) {
			t.Fatalf("source producer %q: %v", producer, err)
		}
	}
	// Existing source producers remain literal argument vectors.
	producer := []string{"/bin/echo", "${width}"}
	legacy, err := resolveShipped(t, shippedSource("x264-fdsrc", nil, producer))
	if err != nil || !slices.Equal(legacy.Pipeline.resolved.producer, producer) {
		t.Fatalf("source producer changed: %+v %v", legacy, err)
	}
}

func TestMalformedTemplateProducersAreRejected(t *testing.T) {
	good := `{"name":"t","input":"stdin","parameters":{"topic":{}},"command":["encoder"],"producer":["/usr/lib/image-pipe","--topic","${topic}"]}`
	if _, err := parsePipelineTemplate([]byte(good)); err != nil {
		t.Fatal(err)
	}
	for name, replacement := range map[string][2]string{
		"device input":           {`"input":"stdin"`, `"input":"device"`},
		"empty vector":           {`["/usr/lib/image-pipe","--topic","${topic}"]`, `[]`},
		"shell line":             {`["/usr/lib/image-pipe","--topic","${topic}"]`, `"/usr/lib/image-pipe --topic /sim/image_raw"`},
		"relative program":       {`/usr/lib/image-pipe`, `bin/pipe`},
		"empty program":          {`/usr/lib/image-pipe`, ``},
		"NUL argument":           {`--topic`, `--topic\u0000`},
		"undeclared parameter":   {`${topic}`, `${unknown}`},
		"stray dollar":           {`${topic}`, `$topic`},
		"unterminated parameter": {`${topic}`, `${topic`},
		"too long argument":      {`--topic`, strings.Repeat("x", maximumPipelineArgumentBytes+1)},
		"too many arguments":     {`["/usr/lib/image-pipe","--topic","${topic}"]`, `["pipe"` + strings.Repeat(`,"x"`, maximumPipelineArguments) + `]`},
	} {
		t.Run(name, func(t *testing.T) {
			document := strings.Replace(good, replacement[0], replacement[1], 1)
			if _, err := parsePipelineTemplate([]byte(document)); err == nil {
				t.Fatalf("accepted %s", document)
			}
		})
	}
}

func TestTemplateParametersAreCheckedBeforeTheyReachAnArgumentVector(t *testing.T) {
	for name, parameters := range map[string]map[string]PipelineValue{
		"missing required device":  {},
		"undeclared parameter":     {"device": "/dev/video0", "extra": "1"},
		"built in parameter":       {"device": "/dev/video0", "width": "1"},
		"pipeline element in path": {"device": "/dev/video0 ! fakesink"},
		"property in a value":      {"device": "/dev/video0", "format": "NV12 bitrate=1"},
		"not a device node":        {"device": "/etc/passwd"},
		"letters for a bitrate":    {"device": "/dev/video0", "bitrateKbps": "fast"},
		"empty value":              {"device": ""},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := resolveShipped(t, shippedSource("x264-v4l2src-raw", parameters, nil)); err == nil {
				t.Fatalf("accepted %v", parameters)
			}
		})
	}
	source, err := resolveShipped(t, shippedSource("x264-v4l2src-raw", map[string]PipelineValue{"device": "/dev/v4l/by-id/usb-Intel_R__RealSense_TM__Depth_Camera_435-video-index0", "format": "YUY2", "bitrateKbps": "4000"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	command := strings.Join(source.Pipeline.resolved.command, " ")
	if !strings.Contains(command, "format=YUY2,width=64,height=48") || !strings.Contains(command, "bitrate=4000") {
		t.Fatalf("parameters were not applied: %s", command)
	}
}

func TestPipelineValuesAcceptStringsAndNumbersOnly(t *testing.T) {
	var config PipelineConfig
	if err := json.Unmarshal([]byte(`{"template":"x264-v4l2src-raw","parameters":{"device":"/dev/video0","bitrateKbps":4000}}`), &config); err != nil {
		t.Fatal(err)
	}
	if config.Parameters["bitrateKbps"] != "4000" {
		t.Fatalf("numeric parameter = %q", config.Parameters["bitrateKbps"])
	}
	for _, document := range []string{
		`{"template":"x","parameters":{"a":true}}`,
		`{"template":"x","parameters":{"a":null}}`,
		`{"template":"x","parameters":{"a":["b"]}}`,
		`{"template":"x","parameters":{"a":{"b":1}}}`,
	} {
		if err := json.Unmarshal([]byte(document), &config); err == nil {
			t.Errorf("accepted %s", document)
		}
	}
}

func TestMalformedTemplatesAreRejected(t *testing.T) {
	good := `{"name":"t","description":"d","input":"device","snapshot":false,"parameters":{"device":{}},"command":["gst-launch-1.0","v4l2src","device=${device}"]}`
	if _, err := parsePipelineTemplate([]byte(good)); err != nil {
		t.Fatalf("baseline template: %v", err)
	}
	for name, replacement := range map[string][2]string{
		"unknown field":               {`"snapshot":false`, `"snapshot":false,"extra":1`},
		"bad name":                    {`"name":"t"`, `"name":"T!"`},
		"bad input":                   {`"input":"device"`, `"input":"socket"`},
		"undeclared placeholder":      {`device=${device}`, `device=${disk}`},
		"stray dollar":                {`device=${device}`, `device=$device`},
		"unterminated placeholder":    {`device=${device}`, `device=${device`},
		"builtin declared":            {`"parameters":{"device":{}}`, `"parameters":{"device":{},"width":{}}`},
		"snapshot without directory":  {`"snapshot":false`, `"snapshot":true`},
		"directory without snapshot":  {`device=${device}`, `device=${device} ${snapshotDir}`},
		"relative program":            {`"gst-launch-1.0"`, `"bin/gst-launch-1.0"`},
		"empty argument":              {`"v4l2src"`, `""`},
		"default breaks own pattern":  {`"device":{}`, `"device":{"default":"x y"}`},
		"pattern does not compile":    {`"device":{}`, `"device":{"pattern":"("}`},
		"command missing":             {`"command":["gst-launch-1.0","v4l2src","device=${device}"]`, `"command":[]`},
		"parameter name not camel":    {`"parameters":{"device":{}}`, `"parameters":{"Device":{}}`},
		"parameter default is a list": {`"device":{}`, `"device":{"default":["a"]}`},
	} {
		t.Run(name, func(t *testing.T) {
			document := strings.Replace(good, replacement[0], replacement[1], 1)
			if document == good {
				t.Fatal("test replacement did not apply")
			}
			if _, err := parsePipelineTemplate([]byte(document)); err == nil {
				t.Fatalf("accepted %s", document)
			}
		})
	}
	if _, err := parsePipelineTemplate([]byte(good + " {}")); err == nil {
		t.Fatal("a template followed by another document was accepted")
	}
	snapshotWithoutPattern := `{"name":"t","description":"d","input":"device","snapshot":true,"parameters":{},"command":["gst-launch-1.0","multifilesink","location=${snapshotDir}/frame.jpg"]}`
	if _, err := parsePipelineTemplate([]byte(snapshotWithoutPattern)); err == nil || !strings.Contains(err.Error(), "snapshot-%05d.jpg") {
		t.Fatalf("snapshot branch with another file name: %v", err)
	}
}

func TestTemplateNameMustMatchItsFile(t *testing.T) {
	directory := t.TempDir()
	document := `{"name":"other","description":"d","input":"device","snapshot":false,"parameters":{},"command":["true"]}`
	if err := os.WriteFile(filepath.Join(directory, "wanted.json"), []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPipelineTemplate(directory, "wanted"); err == nil || !strings.Contains(err.Error(), "declares the name") {
		t.Fatalf("mismatching name: %v", err)
	}
	if _, err := loadPipelineTemplate(directory, "missing"); err == nil {
		t.Fatal("a missing template was loaded")
	}
	if _, err := loadPipelineTemplate(directory, "../wanted"); err == nil {
		t.Fatal("a template name with a path was accepted")
	}
}

func TestPipelineSourceConfigurationIsStrict(t *testing.T) {
	base := func() SourceConfig {
		return shippedSource("x264-v4l2src-raw", map[string]PipelineValue{"device": "/dev/video0"}, nil)
	}
	if _, err := resolveShipped(t, base()); err != nil {
		t.Fatalf("valid pipeline source: %v", err)
	}
	for name, change := range map[string]func(*SourceConfig){
		"control socket":      func(s *SourceConfig) { s.ControlSocket = "/run/x.sock" },
		"no pipeline":         func(s *SourceConfig) { s.Pipeline = nil },
		"no width":            func(s *SourceConfig) { s.Width = 0 },
		"no frame ID":         func(s *SourceConfig) { s.FrameID = "" },
		"fractional rate":     func(s *SourceConfig) { s.FPS = 29.97 },
		"rate out of range":   func(s *SourceConfig) { s.FPS = 500 },
		"unknown kind":        func(s *SourceConfig) { s.Kind = "plugin" },
		"localhost listener":  func(s *SourceConfig) { s.RTPListenAddress = "localhost:5004" },
		"unknown template":    func(s *SourceConfig) { s.Pipeline.Template = "does-not-exist" },
		"template with slash": func(s *SourceConfig) { s.Pipeline.Template = "../x264-fdsrc" },
	} {
		t.Run(name, func(t *testing.T) {
			source := base()
			change(&source)
			if _, err := resolveShipped(t, source); err == nil {
				t.Fatal("accepted an invalid pipeline source")
			}
		})
	}
	// A camera-source must not carry pipeline settings.
	camera := SourceConfig{ID: "camera", RTPListenAddress: "127.0.0.1:5004", ControlSocket: "/run/x.sock", Pipeline: &PipelineConfig{Template: "x264-fdsrc"}}
	if _, err := camera.normalized(); err == nil {
		t.Fatal("a camera-source with pipeline settings was accepted")
	}
	// Directories are required, and canonical, for pipeline sources only.
	if _, err := (Config{ControlAddress: "127.0.0.1:0", Sources: []SourceConfig{base()}}).normalized(); err == nil {
		t.Fatal("pipeline source without template and runtime directories")
	}
	if _, err := (Config{ControlAddress: "127.0.0.1:0", PipelineTemplatesDir: "relative", PipelineRuntimeDir: "/run/x", Sources: []SourceConfig{base()}}).normalized(); err == nil {
		t.Fatal("relative templates directory accepted")
	}
	if _, err := (Config{ControlAddress: "127.0.0.1:0", Sources: []SourceConfig{{ID: "camera", RTPListenAddress: "127.0.0.1:5004", ControlSocket: "/run/x.sock"}}}).normalized(); err != nil {
		t.Fatalf("camera-source without pipeline directories: %v", err)
	}
}

func TestSourcesMayNotShareARuntimeResource(t *testing.T) {
	first := shippedSource("x264-v4l2src-raw", map[string]PipelineValue{"device": "/dev/video0"}, nil)
	second := first
	second.ID = "rear"
	absolute, _ := filepath.Abs(shippedTemplates)
	config := Config{ControlAddress: "127.0.0.1:0", PipelineTemplatesDir: absolute, PipelineRuntimeDir: "/run/x", Sources: []SourceConfig{first, second}}
	if _, err := config.normalized(); err == nil || !strings.Contains(err.Error(), "RTP listener") {
		t.Fatalf("two sources on one RTP port: %v", err)
	}
	second.RTPListenAddress = "127.0.0.1:5006"
	config.Sources = []SourceConfig{first, second}
	if _, err := config.normalized(); err == nil || !strings.Contains(err.Error(), "camera device") {
		t.Fatalf("two sources on one camera device: %v", err)
	}
	second.Pipeline = &PipelineConfig{Template: first.Pipeline.Template, Parameters: map[string]PipelineValue{"device": "/dev/video1"}}
	config.Sources = []SourceConfig{first, second}
	normalized, err := config.normalized()
	if err != nil {
		t.Fatal(err)
	}
	if a, b := normalized.Sources[0].Pipeline.resolved.snapshotDir, normalized.Sources[1].Pipeline.resolved.snapshotDir; a == b {
		t.Fatalf("both sources write snapshots to %s", a)
	}
	// Normalizing must not edit the caller's configuration.
	if first.Pipeline.resolved != nil || first.Kind != SourceKindPipeline {
		t.Fatalf("the input source was modified: %+v", first)
	}
}
