package object

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

const drive9PresignTokenEnvDefault = "JFS_DRIVE9_OBJECT_SIGN_TOKEN"

type drive9PresignStore struct {
	DefaultObjectStorage
	volume   string
	endpoint string
	token    string
	prefix   string
	client   *http.Client
}

type drive9PresignRequest struct {
	Operation     string `json:"operation"`
	Key           string `json:"key,omitempty"`
	ContentLength *int64 `json:"content_length,omitempty"`
	Range         string `json:"range,omitempty"`
	Prefix        string `json:"prefix,omitempty"`
	StartAfter    string `json:"start_after,omitempty"`
	Token         string `json:"token,omitempty"`
	Delimiter     string `json:"delimiter,omitempty"`
	Limit         int64  `json:"limit,omitempty"`
	UploadID      string `json:"upload_id,omitempty"`
	PartNumber    int    `json:"part_number,omitempty"`
}

type drive9PresignPlan struct {
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers,omitempty"`
	ExpiresAt time.Time         `json:"expires_at"`
}

func newDrive9Presign(endpoint, accessKey, secretKey, token string) (ObjectStorage, error) {
	uri, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid drive9-presign endpoint %q: %w", endpoint, err)
	}
	if uri.Scheme != "drive9-presign" {
		return nil, fmt.Errorf("invalid drive9-presign endpoint scheme %q", uri.Scheme)
	}
	values := uri.Query()
	broker := strings.TrimSpace(values.Get("endpoint"))
	if broker == "" {
		return nil, errors.New("drive9-presign endpoint query is required")
	}
	tokenEnv := strings.TrimSpace(values.Get("token_env"))
	if tokenEnv == "" {
		tokenEnv = drive9PresignTokenEnvDefault
	}
	authToken := os.Getenv(tokenEnv)
	if strings.TrimSpace(authToken) == "" {
		return nil, fmt.Errorf("drive9-presign token env %q is empty", tokenEnv)
	}
	if parseDrive9PresignBool(values.Get("multipart")) {
		return nil, errors.New("drive9-presign multipart is disabled until size-aware threshold enforcement exists")
	}
	prefix, err := cleanDrive9PresignPrefix(values.Get("prefix"))
	if err != nil {
		return nil, err
	}
	return &drive9PresignStore{
		volume:   strings.Trim(strings.TrimSpace(uri.Host+uri.Path), "/"),
		endpoint: broker,
		token:    authToken,
		prefix:   prefix,
		client:   httpClient,
	}, nil
}

func (s *drive9PresignStore) String() string {
	if s.volume == "" {
		return "drive9-presign://"
	}
	return "drive9-presign://" + s.volume + "/"
}

func (s *drive9PresignStore) Limits() Limits {
	return DefaultObjectStorage{}.Limits()
}

func (s *drive9PresignStore) Head(ctx context.Context, key string) (Object, error) {
	key, err := cleanDrive9PresignKey(key, true)
	if err != nil {
		return nil, err
	}
	resp, err := s.signedRequest(ctx, drive9PresignRequest{Operation: "head", Key: key}, nil)
	if err != nil {
		return nil, err
	}
	defer cleanup(resp)
	if resp.StatusCode == http.StatusNotFound {
		return nil, os.ErrNotExist
	}
	if !drive9HTTPStatusOK(resp.StatusCode) {
		return nil, parseError(resp)
	}
	size, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid content length: %w", err)
	}
	mtime := time.Now()
	if lastModified := resp.Header.Get("Last-Modified"); lastModified != "" {
		if parsed, err := http.ParseTime(lastModified); err == nil {
			mtime = parsed
		}
	}
	return &obj{key: key, size: size, mtime: mtime, isDir: strings.HasSuffix(key, "/")}, nil
}

func (s *drive9PresignStore) Get(ctx context.Context, key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	key, err := cleanDrive9PresignKey(key, true)
	if err != nil {
		return nil, err
	}
	req := drive9PresignRequest{Operation: "get", Key: key}
	if off > 0 || limit > 0 {
		if limit > 0 {
			req.Range = fmt.Sprintf("bytes=%d-%d", off, off+limit-1)
		} else {
			req.Range = fmt.Sprintf("bytes=%d-", off)
		}
	}
	resp, err := s.signedRequest(ctx, req, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		cleanup(resp)
		return nil, os.ErrNotExist
	}
	if !drive9HTTPStatusOK(resp.StatusCode) {
		defer cleanup(resp)
		return nil, parseError(resp)
	}
	return resp.Body, nil
}

func (s *drive9PresignStore) Put(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) error {
	key, err := cleanDrive9PresignKey(key, true)
	if err != nil {
		return err
	}
	body, size, closeBody, err := drive9PresignBody(in)
	if err != nil {
		return err
	}
	defer closeBody()
	resp, err := s.signedRequest(ctx, drive9PresignRequest{Operation: "put", Key: key, ContentLength: &size}, body)
	if err != nil {
		return err
	}
	defer cleanup(resp)
	if !drive9HTTPStatusOK(resp.StatusCode) {
		return parseError(resp)
	}
	return nil
}

func (s *drive9PresignStore) Copy(ctx context.Context, dst, src string) error {
	return notSupported
}

func (s *drive9PresignStore) Delete(ctx context.Context, key string, getters ...AttrGetter) error {
	key, err := cleanDrive9PresignKey(key, true)
	if err != nil {
		return err
	}
	resp, err := s.signedRequest(ctx, drive9PresignRequest{Operation: "delete", Key: key}, nil)
	if err != nil {
		return err
	}
	defer cleanup(resp)
	if drive9HTTPStatusOK(resp.StatusCode) || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return parseError(resp)
}

func (s *drive9PresignStore) List(ctx context.Context, prefix, startAfter, token, delimiter string, limit int64, followLink bool) ([]Object, bool, string, error) {
	prefix, err := cleanDrive9PresignKey(prefix, false)
	if err != nil {
		return nil, false, "", err
	}
	startAfter, err = cleanDrive9PresignKey(startAfter, false)
	if err != nil {
		return nil, false, "", err
	}
	resp, err := s.signedRequest(ctx, drive9PresignRequest{
		Operation:  "list",
		Prefix:     prefix,
		StartAfter: startAfter,
		Token:      token,
		Delimiter:  delimiter,
		Limit:      limit,
	}, nil)
	if err != nil {
		return nil, false, "", err
	}
	defer cleanup(resp)
	if !drive9HTTPStatusOK(resp.StatusCode) {
		return nil, false, "", parseError(resp)
	}
	var result drive9ListBucketResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, false, "", err
	}
	objects := make([]Object, 0, len(result.Contents)+len(result.CommonPrefixes))
	for _, item := range result.Contents {
		key, ok := s.stripPrefix(item.Key)
		if !ok || key < startAfter {
			return nil, false, "", fmt.Errorf("found invalid key %s from List, prefix: %s, marker: %s", item.Key, prefix, startAfter)
		}
		objects = append(objects, &obj{
			key:   key,
			size:  item.Size,
			mtime: item.lastModified(),
			isDir: strings.HasSuffix(key, "/"),
		})
	}
	for _, item := range result.CommonPrefixes {
		key, ok := s.stripPrefix(item.Prefix)
		if !ok {
			return nil, false, "", fmt.Errorf("found invalid common prefix %s from List, prefix: %s", item.Prefix, prefix)
		}
		objects = append(objects, &obj{key: key, mtime: time.Unix(0, 0), isDir: true})
	}
	return objects, result.IsTruncated, result.NextContinuationToken, nil
}

func (s *drive9PresignStore) ListAll(ctx context.Context, prefix, marker string, followLink bool) (<-chan Object, error) {
	return nil, notSupported
}

func (s *drive9PresignStore) CreateMultipartUpload(ctx context.Context, key string) (*MultipartUpload, error) {
	return nil, notSupported
}

func (s *drive9PresignStore) UploadPart(ctx context.Context, key string, uploadID string, num int, body []byte) (*Part, error) {
	return nil, notSupported
}

func (s *drive9PresignStore) UploadPartCopy(ctx context.Context, key string, uploadID string, num int, srcKey string, off, size int64) (*Part, error) {
	return nil, notSupported
}

func (s *drive9PresignStore) AbortUpload(ctx context.Context, key string, uploadID string) {
}

func (s *drive9PresignStore) CompleteUpload(ctx context.Context, key string, uploadID string, parts []*Part) error {
	return notSupported
}

func (s *drive9PresignStore) ListUploads(ctx context.Context, marker string) ([]*PendingPart, string, error) {
	return nil, "", notSupported
}

func (s *drive9PresignStore) signedRequest(ctx context.Context, req drive9PresignRequest, body io.Reader) (*http.Response, error) {
	plan, err := s.sign(ctx, req)
	if err != nil {
		return nil, err
	}
	method := strings.ToUpper(strings.TrimSpace(plan.Method))
	if method == "" {
		method = http.MethodGet
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, plan.URL, body)
	if err != nil {
		return nil, err
	}
	for name, value := range plan.Headers {
		httpReq.Header.Set(name, value)
		if strings.EqualFold(name, "Content-Length") {
			if n, err := strconv.ParseInt(value, 10, 64); err == nil {
				httpReq.ContentLength = n
			}
		}
	}
	client := s.client
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(httpReq)
}

func (s *drive9PresignStore) sign(ctx context.Context, req drive9PresignRequest) (drive9PresignPlan, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return drive9PresignPlan{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return drive9PresignPlan{}, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+s.token)
	httpReq.Header.Set("Content-Type", "application/json")
	client := s.client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return drive9PresignPlan{}, err
	}
	defer cleanup(resp)
	if !drive9HTTPStatusOK(resp.StatusCode) {
		return drive9PresignPlan{}, parseError(resp)
	}
	var plan drive9PresignPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return drive9PresignPlan{}, err
	}
	if strings.TrimSpace(plan.URL) == "" {
		return drive9PresignPlan{}, errors.New("empty signed URL")
	}
	return plan, nil
}

func (s *drive9PresignStore) stripPrefix(key string) (string, bool) {
	if s.prefix == "" {
		return key, true
	}
	if key == s.prefix {
		return "", true
	}
	prefix := s.prefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", false
	}
	return key[len(prefix):], true
}

type drive9ListBucketResult struct {
	XMLName               xml.Name               `xml:"ListBucketResult"`
	IsTruncated           bool                   `xml:"IsTruncated"`
	NextContinuationToken string                 `xml:"NextContinuationToken"`
	Contents              []drive9ListObject     `xml:"Contents"`
	CommonPrefixes        []drive9CommonPrefixes `xml:"CommonPrefixes"`
}

type drive9ListObject struct {
	Key          string `xml:"Key"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
}

func (o drive9ListObject) lastModified() time.Time {
	if o.LastModified == "" {
		return time.Unix(0, 0)
	}
	if t, err := time.Parse(time.RFC3339, o.LastModified); err == nil {
		return t
	}
	if t, err := http.ParseTime(o.LastModified); err == nil {
		return t
	}
	return time.Unix(0, 0)
}

type drive9CommonPrefixes struct {
	Prefix string `xml:"Prefix"`
}

func drive9PresignBody(in io.Reader) (io.Reader, int64, func(), error) {
	if seeker, ok := in.(io.ReadSeeker); ok {
		cur, err := seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, 0, func() {}, err
		}
		end, err := seeker.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, 0, func() {}, err
		}
		if _, err := seeker.Seek(cur, io.SeekStart); err != nil {
			return nil, 0, func() {}, err
		}
		return seeker, end - cur, func() {}, nil
	}
	file, err := os.CreateTemp("", "juicefs-drive9-presign-put-*")
	if err != nil {
		return nil, 0, func() {}, err
	}
	cleanupFile := func() {
		name := file.Name()
		_ = file.Close()
		_ = os.Remove(name)
	}
	size, err := io.Copy(file, in)
	if err != nil {
		cleanupFile()
		return nil, 0, func() {}, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		cleanupFile()
		return nil, 0, func() {}, err
	}
	return file, size, cleanupFile, nil
}

func cleanDrive9PresignKey(raw string, required bool) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		if required {
			return "", errors.New("object key is required")
		}
		return "", nil
	}
	if strings.HasPrefix(raw, "/") || strings.ContainsAny(raw, "\x00\r\n") || strings.Contains(raw, "\\") {
		return "", fmt.Errorf("invalid object key %q", raw)
	}
	for _, part := range strings.Split(raw, "/") {
		if part == "." || part == ".." {
			return "", fmt.Errorf("invalid object key %q", raw)
		}
	}
	return raw, nil
}

func cleanDrive9PresignPrefix(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if strings.HasPrefix(raw, "/") || strings.ContainsAny(raw, "\x00\r\n") || strings.Contains(raw, "\\") {
		return "", fmt.Errorf("invalid drive9-presign prefix %q", raw)
	}
	for _, part := range strings.Split(raw, "/") {
		if part == "." || part == ".." {
			return "", fmt.Errorf("invalid drive9-presign prefix %q", raw)
		}
	}
	cleaned := strings.Trim(path.Clean("/"+raw), "/")
	if cleaned == "." {
		return "", nil
	}
	for _, part := range strings.Split(cleaned, "/") {
		if part == "." || part == ".." {
			return "", fmt.Errorf("invalid drive9-presign prefix %q", raw)
		}
	}
	return cleaned, nil
}

func parseDrive9PresignBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func drive9HTTPStatusOK(status int) bool {
	return status >= 200 && status < 300
}

func init() {
	Register("drive9-presign", newDrive9Presign)
}
