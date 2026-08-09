package mediamtx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	maximumAPIResponseBytes = 2 << 20
	maximumSDPBytes         = 256 << 10
)

// Client is the loopback-only XGC control boundary to MediaMTX. Browser media
// still travels directly between MediaMTX ICE and the browser.
type Client struct {
	apiBase  *url.URL
	whepBase *url.URL
	http     *http.Client
}

type PathStatus struct {
	Name                 string       `json:"name"`
	Available            bool         `json:"available"`
	Online               bool         `json:"online"`
	InboundBytes         uint64       `json:"inboundBytes"`
	InboundFramesInError uint64       `json:"inboundFramesInError"`
	OutboundBytes        uint64       `json:"outboundBytes"`
	Readers              []PathReader `json:"readers"`
	Tracks               []PathTrack  `json:"tracks2"`
}

type PathReader struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

type PathTrack struct {
	Codec      string         `json:"codec"`
	CodecProps map[string]any `json:"codecProps"`
}

type WebRTCSession struct {
	ID                        string  `json:"id"`
	Path                      string  `json:"path"`
	State                     string  `json:"state"`
	Query                     string  `json:"query"`
	PeerConnectionEstablished bool    `json:"peerConnectionEstablished"`
	InboundBytes              uint64  `json:"inboundBytes"`
	OutboundBytes             uint64  `json:"outboundBytes"`
	InboundRTCPPackets        uint64  `json:"inboundRTCPPackets"`
	OutboundRTCPPackets       uint64  `json:"outboundRTCPPackets"`
	OutboundRTPPackets        uint64  `json:"outboundRTPPackets"`
	InboundRTPPacketsLost     uint64  `json:"inboundRTPPacketsLost"`
	InboundRTPPacketsJitter   float64 `json:"inboundRTPPacketsJitter"`
}

type WHEPSession struct {
	AnswerSDP string
	Location  *url.URL
}

type RecordingSettings struct {
	Enabled         bool
	Path            string
	PartDuration    string
	SegmentDuration string
}

// HTTPError preserves the upstream status while keeping the response bounded.
type HTTPError struct {
	Operation string
	Status    int
	Message   string
}

func (err *HTTPError) Error() string {
	return fmt.Sprintf("MediaMTX %s returned HTTP %d: %s", err.Operation, err.Status, err.Message)
}

func NewClient(apiBase string, whepBase string) (*Client, error) {
	api, err := parseLoopbackHTTPBase(apiBase)
	if err != nil {
		return nil, fmt.Errorf("MediaMTX API URL: %w", err)
	}
	whep, err := parseLoopbackHTTPBase(whepBase)
	if err != nil {
		return nil, fmt.Errorf("MediaMTX WHEP URL: %w", err)
	}
	return &Client{
		apiBase: api, whepBase: whep,
		http: &http.Client{
			Timeout: 15 * time.Second,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return errors.New("MediaMTX redirects are not allowed")
			},
		},
	}, nil
}

func parseLoopbackHTTPBase(value string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil {
		return nil, errors.New("must be an unauthenticated http URL")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("query and fragment are not allowed")
	}
	host := parsed.Hostname()
	if host != "localhost" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return nil, errors.New("must target a loopback host")
		}
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	return parsed, nil
}

func (client *Client) Paths(ctx context.Context) ([]PathStatus, error) {
	var response struct {
		Items []PathStatus `json:"items"`
	}
	if err := client.apiJSON(ctx, http.MethodGet, "/v3/paths/list", nil, &response); err != nil {
		return nil, err
	}
	return response.Items, nil
}

func (client *Client) Path(ctx context.Context, name string) (PathStatus, error) {
	if !pathName.MatchString(name) {
		return PathStatus{}, errors.New("MediaMTX path name is invalid")
	}
	var response PathStatus
	if err := client.apiJSON(ctx, http.MethodGet, "/v3/paths/get/"+url.PathEscape(name), nil, &response); err != nil {
		return PathStatus{}, err
	}
	return response, nil
}

func (client *Client) WebRTCSessions(ctx context.Context) ([]WebRTCSession, error) {
	var response struct {
		Items []WebRTCSession `json:"items"`
	}
	if err := client.apiJSON(ctx, http.MethodGet, "/v3/webrtcsessions/list", nil, &response); err != nil {
		return nil, err
	}
	return response.Items, nil
}

func (client *Client) SetRecording(ctx context.Context, name string, enabled bool) error {
	return client.ConfigureRecording(ctx, name, RecordingSettings{Enabled: enabled})
}

func (client *Client) ConfigureRecording(ctx context.Context, name string, settings RecordingSettings) error {
	if !pathName.MatchString(name) {
		return errors.New("MediaMTX path name is invalid")
	}
	input := struct {
		Record                bool   `json:"record"`
		RecordPath            string `json:"recordPath,omitempty"`
		RecordFormat          string `json:"recordFormat,omitempty"`
		RecordPartDuration    string `json:"recordPartDuration,omitempty"`
		RecordSegmentDuration string `json:"recordSegmentDuration,omitempty"`
	}{
		Record: settings.Enabled, RecordPath: strings.TrimSpace(settings.Path),
		RecordPartDuration:    strings.TrimSpace(settings.PartDuration),
		RecordSegmentDuration: strings.TrimSpace(settings.SegmentDuration),
	}
	if input.RecordPath != "" {
		if !strings.HasPrefix(input.RecordPath, "/") {
			return errors.New("MediaMTX recording path must be absolute")
		}
		input.RecordFormat = "fmp4"
	}
	return client.apiJSON(ctx, http.MethodPatch, "/v3/config/paths/patch/"+url.PathEscape(name), input, nil)
}

func (client *Client) OpenWHEP(
	ctx context.Context,
	name string,
	offerSDP string,
	sessionToken string,
) (WHEPSession, error) {
	if !pathName.MatchString(name) {
		return WHEPSession{}, errors.New("MediaMTX path name is invalid")
	}
	if strings.TrimSpace(offerSDP) == "" || len(offerSDP) > maximumSDPBytes {
		return WHEPSession{}, errors.New("WHEP offer SDP is required and must be at most 256 KiB")
	}
	sessionToken = strings.TrimSpace(sessionToken)
	if sessionToken == "" || len(sessionToken) > 128 {
		return WHEPSession{}, errors.New("XGC WHEP session token is required and must be at most 128 bytes")
	}
	endpoint := client.resolveWHEP("/" + url.PathEscape(name) + "/whep")
	query := endpoint.Query()
	query.Set("xgcSession", sessionToken)
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(offerSDP))
	if err != nil {
		return WHEPSession{}, err
	}
	request.Header.Set("Content-Type", "application/sdp")
	request.Header.Set("Accept", "application/sdp")
	response, err := client.http.Do(request)
	if err != nil {
		return WHEPSession{}, fmt.Errorf("open MediaMTX WHEP session: %w", err)
	}
	defer response.Body.Close()
	body, err := readBounded(response.Body, maximumSDPBytes)
	if err != nil {
		return WHEPSession{}, fmt.Errorf("read MediaMTX WHEP answer: %w", err)
	}
	if response.StatusCode != http.StatusCreated {
		return WHEPSession{}, upstreamError("WHEP POST", response.StatusCode, body)
	}
	if !strings.HasPrefix(strings.ToLower(response.Header.Get("Content-Type")), "application/sdp") {
		return WHEPSession{}, errors.New("MediaMTX WHEP answer is not application/sdp")
	}
	if strings.TrimSpace(string(body)) == "" {
		return WHEPSession{}, errors.New("MediaMTX WHEP answer SDP is empty")
	}
	location, err := client.validateWHEPLocation(name, sessionToken, response.Header.Get("Location"))
	if err != nil {
		return WHEPSession{}, err
	}
	return WHEPSession{AnswerSDP: string(body), Location: location}, nil
}

func (client *Client) CloseWHEP(ctx context.Context, location *url.URL) (bool, error) {
	if err := client.validateExistingWHEPLocation(location); err != nil {
		return false, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, location.String(), nil)
	if err != nil {
		return false, err
	}
	response, err := client.http.Do(request)
	if err != nil {
		return false, fmt.Errorf("close MediaMTX WHEP session: %w", err)
	}
	defer response.Body.Close()
	body, readErr := readBounded(response.Body, maximumAPIResponseBytes)
	if readErr != nil {
		return false, readErr
	}
	if response.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		return false, upstreamError("WHEP DELETE", response.StatusCode, body)
	}
	return true, nil
}

func (client *Client) apiJSON(ctx context.Context, method string, path string, input any, output any) error {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	endpoint := *client.apiBase
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.http.Do(request)
	if err != nil {
		return fmt.Errorf("call MediaMTX API: %w", err)
	}
	defer response.Body.Close()
	content, err := readBounded(response.Body, maximumAPIResponseBytes)
	if err != nil {
		return fmt.Errorf("read MediaMTX API response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return upstreamError(method+" "+path, response.StatusCode, content)
	}
	if output == nil || len(bytes.TrimSpace(content)) == 0 {
		return nil
	}
	if err := json.Unmarshal(content, output); err != nil {
		return fmt.Errorf("decode MediaMTX API response: %w", err)
	}
	return nil
}

func (client *Client) resolveWHEP(path string) *url.URL {
	endpoint := *client.whepBase
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	return &endpoint
}

func (client *Client) validateWHEPLocation(name string, sessionToken string, value string) (*url.URL, error) {
	if strings.TrimSpace(value) == "" {
		return nil, errors.New("MediaMTX WHEP response has no Location")
	}
	reference, err := url.Parse(value)
	if err != nil {
		return nil, errors.New("MediaMTX WHEP Location is invalid")
	}
	location := client.whepBase.ResolveReference(reference)
	if err := client.validateExistingWHEPLocation(location); err != nil {
		return nil, err
	}
	expectedPrefix := strings.TrimRight(client.whepBase.Path, "/") + "/" + url.PathEscape(name) + "/whep/"
	if !strings.HasPrefix(location.EscapedPath(), expectedPrefix) || location.EscapedPath() == expectedPrefix {
		return nil, errors.New("MediaMTX WHEP Location is outside the requested source")
	}
	if location.RawQuery != "" {
		query, _ := url.ParseQuery(location.RawQuery)
		if query.Get("xgcSession") != sessionToken {
			return nil, errors.New("MediaMTX WHEP Location changed the XGC session token")
		}
	}
	return location, nil
}

func (client *Client) validateExistingWHEPLocation(location *url.URL) error {
	if location == nil || location.Scheme != client.whepBase.Scheme ||
		!strings.EqualFold(location.Host, client.whepBase.Host) || location.User != nil ||
		location.Fragment != "" {
		return errors.New("MediaMTX WHEP Location is outside the configured loopback server")
	}
	if location.RawQuery != "" {
		query, err := url.ParseQuery(location.RawQuery)
		if err != nil || len(query) != 1 || len(query["xgcSession"]) != 1 ||
			strings.TrimSpace(query.Get("xgcSession")) == "" || len(query.Get("xgcSession")) > 128 {
			return errors.New("MediaMTX WHEP Location has an invalid XGC session query")
		}
	}
	return nil
}

func readBounded(reader io.Reader, maximum int64) ([]byte, error) {
	content, err := io.ReadAll(io.LimitReader(reader, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > maximum {
		return nil, errors.New("response is too large")
	}
	return content, nil
}

func upstreamError(operation string, status int, content []byte) error {
	message := strings.TrimSpace(string(content))
	var response struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(content, &response) == nil && strings.TrimSpace(response.Error) != "" {
		message = strings.TrimSpace(response.Error)
	}
	if message == "" {
		message = http.StatusText(status)
	}
	if len(message) > 512 {
		message = message[:512]
	}
	return &HTTPError{Operation: operation, Status: status, Message: message}
}
