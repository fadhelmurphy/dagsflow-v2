package dag

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func clearAWSEnv(t *testing.T) {
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION", "AWS_DEFAULT_REGION",
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "AWS_ENDPOINT_URL_STS", "AWS_STS_ENDPOINT"} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	resetAWSCache()
	t.Cleanup(resetAWSCache)
}

func stsXML(action, key string) string {
	exp := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	return `<` + action + `Response xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><` + action + `Result><Credentials>` +
		`<AccessKeyId>` + key + `</AccessKeyId><SecretAccessKey>S-` + key + `</SecretAccessKey><SessionToken>T-` + key + `</SessionToken>` +
		`<Expiration>` + exp + `</Expiration></Credentials></` + action + `Result></` + action + `Response>`
}

func TestAWSWebIdentityAndAssumeRole(t *testing.T) {
	clearAWSEnv(t)
	var calls atomic.Int32
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		b, _ := io.ReadAll(r.Body)
		f, _ := url.ParseQuery(string(b))
		switch f.Get("Action") {
		case "AssumeRoleWithWebIdentity":
			if f.Get("WebIdentityToken") != "jwt-123" || f.Get("RoleArn") != "arn:aws:iam::1:role/pod" || r.Header.Get("Authorization") != "" {
				w.WriteHeader(403)
				return
			}
			_, _ = w.Write([]byte(stsXML("AssumeRoleWithWebIdentity", "WEB")))
		case "AssumeRole":
			// must be signed with the web identity credentials
			if !strings.Contains(r.Header.Get("Authorization"), "Credential=WEB/") || r.Header.Get("X-Amz-Security-Token") != "T-WEB" ||
				f.Get("ExternalId") != "ext" {
				w.WriteHeader(403)
				_, _ = w.Write([]byte("<Error><Message>not signed</Message></Error>"))
				return
			}
			_, _ = w.Write([]byte(stsXML("AssumeRole", "XACCT")))
		}
	}))
	defer sts.Close()
	tok := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(tok, []byte("jwt-123\n"), 0o600)
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", tok)
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::1:role/pod")
	t.Setenv("AWS_ENDPOINT_URL_STS", sts.URL)
	t.Setenv("AWS_REGION", "ap-southeast-3")

	cr, err := awsCredentialsCtx(context.Background(), "nope")
	if err != nil {
		t.Fatal(err)
	}
	if cr.Key != "WEB" || cr.Secret != "S-WEB" || cr.Token != "T-WEB" || cr.Region != "ap-southeast-3" {
		t.Fatalf("web identity creds: %+v", cr)
	}
	if _, err := awsCredentialsCtx(context.Background(), "nope"); err != nil || calls.Load() != 1 {
		t.Fatalf("second call should use the cache: calls=%d err=%v", calls.Load(), err)
	}

	old := SecretsBackends()
	SetSecretsBackends(EnvBackend{})
	defer SetSecretsBackends(old...)
	t.Setenv("DAGSFLOW_CONN_XACCT", `{"Type":"aws","Config":{"role_arn":"arn:aws:iam::2:role/writer","external_id":"ext"}}`)
	cr, err = awsCredentialsCtx(context.Background(), "xacct")
	if err != nil {
		t.Fatal(err)
	}
	if cr.Key != "XACCT" || cr.Token != "T-XACCT" {
		t.Fatalf("assumed role creds: %+v", cr)
	}
}

func TestAWSContainerCredentials(t *testing.T) {
	clearAWSEnv(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "pod-token" {
			w.WriteHeader(401)
			return
		}
		_, _ = w.Write([]byte(`{"AccessKeyId":"ECS","SecretAccessKey":"S","Token":"T","Expiration":"` + time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + `"}`))
	}))
	defer srv.Close()
	tf := filepath.Join(t.TempDir(), "t")
	_ = os.WriteFile(tf, []byte("pod-token"), 0o600)
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", srv.URL+"/v1/credentials")
	t.Setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", tf)
	cr, err := awsCredentialsCtx(context.Background(), "")
	if err != nil || cr.Key != "ECS" || cr.Token != "T" {
		t.Fatalf("container creds: %+v %v", cr, err)
	}
}

func TestAWSInstanceProfileIMDSv2(t *testing.T) {
	clearAWSEnv(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/latest/api/token" {
			if r.Method != http.MethodPut || r.Header.Get("X-aws-ec2-metadata-token-ttl-seconds") == "" {
				w.WriteHeader(400)
				return
			}
			_, _ = w.Write([]byte("imds-tok"))
			return
		}
		if r.Header.Get("X-aws-ec2-metadata-token") != "imds-tok" { // IMDSv1 is refused
			w.WriteHeader(401)
			return
		}
		switch r.URL.Path {
		case "/latest/meta-data/iam/security-credentials/":
			_, _ = w.Write([]byte("etl-role\n"))
		case "/latest/meta-data/iam/security-credentials/etl-role":
			_, _ = w.Write([]byte(`{"Code":"Success","AccessKeyId":"EC2","SecretAccessKey":"S","Token":"T","Expiration":"` + time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + `"}`))
		case "/latest/meta-data/placement/region":
			_, _ = w.Write([]byte("eu-central-1"))
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	t.Setenv("AWS_EC2_METADATA_DISABLED", "")
	t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", srv.URL)
	cr, err := awsCredentialsCtx(context.Background(), "")
	if err != nil || cr.Key != "EC2" || cr.Token != "T" || cr.Region != "eu-central-1" {
		t.Fatalf("imds creds: %+v %v", cr, err)
	}
}

func TestAWSNoCredentials(t *testing.T) {
	clearAWSEnv(t)
	_, err := awsCredentialsCtx(context.Background(), "")
	if err == nil || !strings.Contains(err.Error(), "no AWS credentials") || !strings.Contains(err.Error(), "web identity") {
		t.Fatalf("err = %v", err)
	}
}
