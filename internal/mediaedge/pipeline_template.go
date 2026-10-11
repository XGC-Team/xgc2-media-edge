package mediaedge

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// A pipeline source runs an encoder pipeline that media-edge itself starts when
// the first consumer arrives and stops after the last one left. What runs is an
// argument vector, never a shell line: a template file supplies the vector with
// ${name} placeholders, the source supplies the values.
//
// Templates are shipped per platform (software x264, Jetson nvv4l2h264enc,
// Rockchip mpph264enc), each in a variant that opens a V4L2 device and one that
// reads raw frames from standard input. A template with a snapshot branch has
// the pipeline write numbered JPEG files into a scratch directory of the source;
// that is what makes capture available.

const (
	pipelineInputDevice = "device"
	pipelineInputStdin  = "stdin"

	maximumPipelineTemplateBytes = 64 << 10
	maximumPipelineArguments     = 128
	maximumPipelineArgumentBytes = 1024

	// snapshotFilePattern is the file name pattern, with the frame index, of
	// the snapshot branch of a template.
	snapshotFilePattern = "snapshot-%05d.jpg"
)

var (
	pipelineTemplateName  = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)
	pipelineParameterName = regexp.MustCompile(`^[a-z][A-Za-z0-9]{0,31}$`)
	pipelinePlaceholder   = regexp.MustCompile(`\$\{[^}]*\}`)
	// The default value pattern cannot form a shell word, a GStreamer
	// separator or a second property assignment out of a parameter.
	defaultPipelineValue = `[A-Za-z0-9_./:+,=@%-]{1,256}`
)

// builtinPipelineParameters are filled in from the source. A template cannot
// declare them and a source cannot set them.
var builtinPipelineParameters = map[string]struct{}{
	"width": {}, "height": {}, "fps": {}, "rtpHost": {}, "rtpPort": {},
	"payloadType": {}, "snapshotDir": {},
}

// PipelineValue is a template parameter value. JSON strings and numbers are
// accepted and kept as written.
type PipelineValue string

func (value *PipelineValue) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) > 0 && data[0] == '"' {
		var text string
		if err := json.Unmarshal(data, &text); err != nil {
			return err
		}
		*value = PipelineValue(text)
		return nil
	}
	var number json.Number
	if err := json.Unmarshal(data, &number); err != nil || len(data) == 0 || data[0] == 'n' {
		return errors.New("a pipeline parameter must be a string or a number")
	}
	*value = PipelineValue(number.String())
	return nil
}

// PipelineConfig selects and parameterizes the template of a pipeline source.
type PipelineConfig struct {
	// Template is the name of a template file, without its .json suffix.
	Template string `json:"template"`
	// Parameters are the values of the template's own parameters.
	Parameters map[string]PipelineValue `json:"parameters,omitempty"`
	// Producer is the command whose standard output the pipeline reads as raw
	// video. It is required by templates that read standard input and refused
	// by the others.
	Producer []string `json:"producer,omitempty"`

	resolved *pipelineSpec
}

// pipelineSpec is a template with all values filled in.
type pipelineSpec struct {
	template string
	command  []string
	producer []string
	// snapshotDir is where the snapshot branch writes JPEG files; empty when
	// the template has none.
	snapshotDir string
}

func (config PipelineConfig) normalized() (PipelineConfig, error) {
	config.Template = strings.TrimSpace(config.Template)
	if !pipelineTemplateName.MatchString(config.Template) {
		return PipelineConfig{}, errors.New("pipeline template must be a lower-case template name")
	}
	parameters := make(map[string]PipelineValue, len(config.Parameters))
	for name, value := range config.Parameters {
		if !pipelineParameterName.MatchString(name) {
			return PipelineConfig{}, fmt.Errorf("pipeline parameter name %q is invalid", name)
		}
		parameters[name] = value
	}
	config.Parameters = parameters
	if err := checkPipelineArguments("pipeline producer", config.Producer); err != nil {
		return PipelineConfig{}, err
	}
	config.Producer = append([]string(nil), config.Producer...)
	return config, nil
}

func checkPipelineArguments(what string, arguments []string) error {
	if len(arguments) > maximumPipelineArguments {
		return fmt.Errorf("%s has more than %d arguments", what, maximumPipelineArguments)
	}
	for _, argument := range arguments {
		if argument == "" || len(argument) > maximumPipelineArgumentBytes || strings.ContainsRune(argument, 0) {
			return fmt.Errorf("%s arguments must be non-empty, free of NUL and at most %d bytes", what, maximumPipelineArgumentBytes)
		}
	}
	return nil
}

// resolvePipeline loads the template of a pipeline source and fills it in.
func (config Config) resolvePipeline(source SourceConfig) (SourceConfig, error) {
	for _, directory := range []string{config.PipelineTemplatesDir, config.PipelineRuntimeDir} {
		if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
			return SourceConfig{}, errors.New("pipeline sources require canonical absolute pipeline templates and runtime directories")
		}
	}
	template, err := loadPipelineTemplate(config.PipelineTemplatesDir, source.Pipeline.Template)
	if err != nil {
		return SourceConfig{}, fmt.Errorf("media source %q: %w", source.ID, err)
	}
	spec, err := template.resolve(source, filepath.Join(config.PipelineRuntimeDir, source.ID))
	if err != nil {
		return SourceConfig{}, fmt.Errorf("media source %q: %w", source.ID, err)
	}
	pipeline := *source.Pipeline
	pipeline.resolved = spec
	source.Pipeline = &pipeline
	return source, nil
}

type pipelineTemplate struct {
	name       string
	input      string
	snapshot   bool
	parameters map[string]pipelineParameter
	command    []string
}

type pipelineParameter struct {
	defaultValue *string
	pattern      *regexp.Regexp
}

type pipelineTemplateDocument struct {
	Name        string                               `json:"name"`
	Description string                               `json:"description"`
	Input       string                               `json:"input"`
	Snapshot    bool                                 `json:"snapshot"`
	Parameters  map[string]pipelineParameterDocument `json:"parameters"`
	Command     []string                             `json:"command"`
}

type pipelineParameterDocument struct {
	Description string         `json:"description"`
	Default     *PipelineValue `json:"default"`
	Pattern     string         `json:"pattern"`
}

func loadPipelineTemplate(directory, name string) (*pipelineTemplate, error) {
	if !pipelineTemplateName.MatchString(name) {
		return nil, fmt.Errorf("pipeline template name %q is invalid", name)
	}
	path := filepath.Join(directory, name+".json")
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open pipeline template %q: %w", name, err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maximumPipelineTemplateBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read pipeline template %q: %w", name, err)
	}
	if len(raw) > maximumPipelineTemplateBytes {
		return nil, fmt.Errorf("pipeline template %q exceeds %d bytes", name, maximumPipelineTemplateBytes)
	}
	template, err := parsePipelineTemplate(raw)
	if err != nil {
		return nil, fmt.Errorf("pipeline template %q: %w", name, err)
	}
	if template.name != name {
		return nil, fmt.Errorf("pipeline template file %q declares the name %q", name, template.name)
	}
	return template, nil
}

func parsePipelineTemplate(raw []byte) (*pipelineTemplate, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var document pipelineTemplateDocument
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("must contain exactly one JSON document")
	}
	if !pipelineTemplateName.MatchString(document.Name) {
		return nil, fmt.Errorf("name %q is invalid", document.Name)
	}
	if document.Input != pipelineInputDevice && document.Input != pipelineInputStdin {
		return nil, fmt.Errorf("input must be %q or %q", pipelineInputDevice, pipelineInputStdin)
	}
	template := &pipelineTemplate{
		name: document.Name, input: document.Input, snapshot: document.Snapshot,
		parameters: make(map[string]pipelineParameter, len(document.Parameters)),
		command:    document.Command,
	}
	for name, declared := range document.Parameters {
		if !pipelineParameterName.MatchString(name) {
			return nil, fmt.Errorf("parameter name %q is invalid", name)
		}
		if _, builtin := builtinPipelineParameters[name]; builtin {
			return nil, fmt.Errorf("parameter %q is built in and cannot be declared", name)
		}
		pattern := declared.Pattern
		if pattern == "" {
			pattern = defaultPipelineValue
		}
		compiled, err := regexp.Compile(`^(?:` + pattern + `)$`)
		if err != nil {
			return nil, fmt.Errorf("parameter %q pattern: %w", name, err)
		}
		parameter := pipelineParameter{pattern: compiled}
		if declared.Default != nil {
			value := string(*declared.Default)
			if !compiled.MatchString(value) {
				return nil, fmt.Errorf("parameter %q default %q does not match its pattern", name, value)
			}
			parameter.defaultValue = &value
		}
		template.parameters[name] = parameter
	}
	if len(document.Command) == 0 {
		return nil, errors.New("command is required")
	}
	if err := checkPipelineArguments("command", document.Command); err != nil {
		return nil, err
	}
	if document.Command[0] != filepath.Base(document.Command[0]) && !filepath.IsAbs(document.Command[0]) {
		return nil, errors.New("the command program must be a bare name or an absolute path")
	}
	usesSnapshot := false
	for _, argument := range document.Command {
		placeholders := pipelinePlaceholder.FindAllString(argument, -1)
		if strings.Count(argument, "$") != len(placeholders) {
			return nil, fmt.Errorf("argument %q has a $ that is not a ${name} placeholder", argument)
		}
		for _, placeholder := range placeholders {
			name := placeholder[2 : len(placeholder)-1]
			if _, builtin := builtinPipelineParameters[name]; !builtin {
				if _, declared := template.parameters[name]; !declared {
					return nil, fmt.Errorf("argument %q uses the undeclared parameter %q", argument, name)
				}
			}
			usesSnapshot = usesSnapshot || name == "snapshotDir"
		}
	}
	if usesSnapshot != template.snapshot {
		return nil, errors.New("snapshot must be true exactly when the command uses ${snapshotDir}")
	}
	if template.snapshot && !slices.ContainsFunc(document.Command, func(argument string) bool {
		return strings.Contains(argument, "${snapshotDir}/"+snapshotFilePattern)
	}) {
		return nil, fmt.Errorf("a snapshot branch must write ${snapshotDir}/%s", snapshotFilePattern)
	}
	return template, nil
}

func (template *pipelineTemplate) resolve(source SourceConfig, scratchDir string) (*pipelineSpec, error) {
	pipeline := source.Pipeline
	if template.input == pipelineInputStdin && len(pipeline.Producer) == 0 {
		return nil, fmt.Errorf("template %q reads video from standard input and needs a producer", template.name)
	}
	if template.input != pipelineInputStdin && len(pipeline.Producer) != 0 {
		return nil, fmt.Errorf("template %q opens its own input and takes no producer", template.name)
	}
	for name := range pipeline.Parameters {
		if _, declared := template.parameters[name]; !declared {
			return nil, fmt.Errorf("template %q has no parameter %q", template.name, name)
		}
	}
	host, port, err := net.SplitHostPort(source.RTPListenAddress)
	if err != nil {
		return nil, err
	}
	values := map[string]string{
		"width": strconv.Itoa(source.Width), "height": strconv.Itoa(source.Height),
		"fps": strconv.Itoa(int(source.FPS)), "rtpHost": host, "rtpPort": port,
		"payloadType": strconv.Itoa(sourceRTPPayloadType),
	}
	spec := &pipelineSpec{template: template.name, producer: append([]string(nil), pipeline.Producer...)}
	if template.snapshot {
		values["snapshotDir"] = scratchDir
		spec.snapshotDir = scratchDir
	}
	names := make([]string, 0, len(template.parameters))
	for name := range template.parameters {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		declared := template.parameters[name]
		var value string
		if given, found := pipeline.Parameters[name]; found {
			value = string(given)
		} else if declared.defaultValue != nil {
			value = *declared.defaultValue
		} else {
			return nil, fmt.Errorf("template %q requires the parameter %q", template.name, name)
		}
		if !declared.pattern.MatchString(value) {
			return nil, fmt.Errorf("template %q parameter %q value %q is not allowed", template.name, name, value)
		}
		values[name] = value
	}
	spec.command = make([]string, len(template.command))
	for index, argument := range template.command {
		spec.command[index] = pipelinePlaceholder.ReplaceAllStringFunc(argument, func(placeholder string) string {
			return values[placeholder[2:len(placeholder)-1]]
		})
	}
	return spec, nil
}
