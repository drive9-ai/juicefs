package object

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestDrive9PresignPutUsesBrokerAndNoLongLivedCredentials(t *testing.T) {
	var gotAuth string
	var gotSign drive9PresignRequest
	var gotPayload string

	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("s3 method = %s", r.Method)
		}
		if r.Header.Get("Content-Length") != "11" {
			t.Fatalf("s3 content-length = %q", r.Header.Get("Content-Length"))
		}
		payload, _ := io.ReadAll(r.Body)
		gotPayload = string(payload)
		w.WriteHeader(http.StatusOK)
	}))
	defer s3.Close()

	broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotSign); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(drive9PresignPlan{
			Method:    http.MethodPut,
			URL:       s3.URL + "/put-object",
			Headers:   map[string]string{"Content-Length": "11"},
			ExpiresAt: time.Now().Add(time.Minute),
		})
	}))
	defer broker.Close()

	t.Setenv("JFS_DRIVE9_OBJECT_SIGN_TOKEN", "session-token")
	store := newDrive9PresignStoreForTest(t, broker.URL, "tenants/t1/objects", false)
	if err := store.Put(context.Background(), "objects/vol/1", strings.NewReader("hello world"), nil); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if gotAuth != "Bearer session-token" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotSign.Operation != "put" || gotSign.Key != "objects/vol/1" || gotSign.ContentLength == nil || *gotSign.ContentLength != 11 {
		t.Fatalf("sign request = %#v", gotSign)
	}
	if gotPayload != "hello world" {
		t.Fatalf("payload = %q", gotPayload)
	}
}

func TestDrive9PresignListStripsTenantPrefix(t *testing.T) {
	var gotSign drive9PresignRequest
	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, `<ListBucketResult>
<IsTruncated>true</IsTruncated>
<NextContinuationToken>next-token</NextContinuationToken>
<Contents><Key>tenants/t1/objects/objects/vol/1</Key><LastModified>2026-06-01T00:00:00Z</LastModified><Size>12</Size></Contents>
<CommonPrefixes><Prefix>tenants/t1/objects/objects/vol/dir/</Prefix></CommonPrefixes>
</ListBucketResult>`)
	}))
	defer s3.Close()
	broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotSign); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(drive9PresignPlan{Method: http.MethodGet, URL: s3.URL, ExpiresAt: time.Now().Add(time.Minute)})
	}))
	defer broker.Close()

	t.Setenv("JFS_DRIVE9_OBJECT_SIGN_TOKEN", "session-token")
	store := newDrive9PresignStoreForTest(t, broker.URL, "tenants/t1/objects", false)
	objects, more, token, err := store.List(context.Background(), "objects/vol/", "objects/vol/0", "", "/", 10, false)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if gotSign.Operation != "list" || gotSign.Prefix != "objects/vol/" || gotSign.StartAfter != "objects/vol/0" || gotSign.Delimiter != "/" || gotSign.Limit != 10 {
		t.Fatalf("sign request = %#v", gotSign)
	}
	if !more || token != "next-token" {
		t.Fatalf("more/token = %v/%q", more, token)
	}
	if len(objects) != 2 || objects[0].Key() != "objects/vol/1" || objects[0].Size() != 12 || objects[1].Key() != "objects/vol/dir/" || !objects[1].IsDir() {
		t.Fatalf("objects = %#v", MarshalObjectSliceForTest(objects))
	}
}

func TestDrive9PresignMultipartFailsClosed(t *testing.T) {
	t.Setenv("JFS_DRIVE9_OBJECT_SIGN_TOKEN", "session-token")
	if _, err := newDrive9PresignStoreForTestErr("http://127.0.0.1:1/sign", "tenants/t1/objects", true); err == nil || !strings.Contains(err.Error(), "multipart is disabled") {
		t.Fatalf("newDrive9Presign multipart error = %v, want disabled", err)
	}
	store := newDrive9PresignStoreForTest(t, "http://127.0.0.1:1/sign", "tenants/t1/objects", false)
	if limits := store.Limits(); limits.IsSupportMultipartUpload {
		t.Fatalf("multipart unexpectedly supported: %#v", limits)
	}
	if _, err := store.CreateMultipartUpload(context.Background(), "objects/vol/2"); !errors.Is(err, notSupported) {
		t.Fatalf("CreateMultipartUpload error = %v, want notSupported", err)
	}
}

func TestDrive9PresignRejectsUnsafeKeys(t *testing.T) {
	t.Setenv("JFS_DRIVE9_OBJECT_SIGN_TOKEN", "session-token")
	store := newDrive9PresignStoreForTest(t, "http://127.0.0.1:1/sign", "tenants/t1/objects", false)
	for _, key := range []string{"../x", "a/../x", "./x", "a/./x", "/absolute"} {
		if err := store.Put(context.Background(), key, strings.NewReader("payload")); err == nil {
			t.Fatalf("Put(%q) succeeded, want error", key)
		}
	}
}

func newDrive9PresignStoreForTest(t *testing.T, broker string, prefix string, multipart bool) *drive9PresignStore {
	t.Helper()
	storage, err := newDrive9PresignStoreForTestErr(broker, prefix, multipart)
	if err != nil {
		t.Fatal(err)
	}
	store, ok := storage.(*drive9PresignStore)
	if !ok {
		t.Fatalf("storage type = %T", storage)
	}
	return store
}

func newDrive9PresignStoreForTestErr(broker string, prefix string, multipart bool) (ObjectStorage, error) {
	endpoint := url.URL{Scheme: "drive9-presign", Host: "vol-test"}
	values := endpoint.Query()
	values.Set("endpoint", broker)
	values.Set("token_env", "JFS_DRIVE9_OBJECT_SIGN_TOKEN")
	values.Set("prefix", prefix)
	if multipart {
		values.Set("multipart", "true")
	}
	endpoint.RawQuery = values.Encode()
	return newDrive9Presign(endpoint.String(), "", "", "")
}

func MarshalObjectSliceForTest(objects []Object) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(objects))
	for _, object := range objects {
		out = append(out, MarshalObject(object))
	}
	return out
}
