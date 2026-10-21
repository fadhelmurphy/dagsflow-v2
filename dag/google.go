package dag

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// Google APIs over plain REST: a service account (key file or JSON in the
// connection) is exchanged for an access token with a signed JWT, so the
// BigQuery rows / Google Sheets nodes need no extra dependencies. On GCE, GKE
// and Cloud Composer, with no key configured, the metadata server is used.

const (
	scopeBigQuery    = "https://www.googleapis.com/auth/bigquery"
	scopeSheets      = "https://www.googleapis.com/auth/spreadsheets"
	scopeSheetsRead  = "https://www.googleapis.com/auth/spreadsheets.readonly"
	defaultGoogleCon = "google_cloud_default"
)

var (
	bqBase      = "https://bigquery.googleapis.com/bigquery/v2"
	sheetsBase  = "https://sheets.googleapis.com/v4/spreadsheets"
	metadataURL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
)

type saKey struct {
	Type        string `json:"type"`
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
	TokenURI    string `json:"token_uri"`
	ProjectID   string `json:"project_id"`
}

type cachedToken struct {
	tok string
	exp time.Time
}

var (
	tokMu  sync.Mutex
	tokens = map[string]cachedToken{}
)

// googleCreds resolves the key of a connection, in this order:
//   - credentials_json (string or object) or credentials_path (file),
//   - the Airflow names: keyfile_dict / key_path, also with the
//     extra__google_cloud_platform__ prefix, or inside an "extra" object/string,
//   - the connection config being the service account key itself
//     (the downloaded JSON pasted as the connection),
//   - GOOGLE_APPLICATION_CREDENTIALS.
func googleCreds(c Connection) (*saKey, error) {
	var raw []byte
	cfg := c.Config
	if ex, ok := cfg["extra"]; ok { // Airflow export: extra is a JSON string or object
		var m map[string]any
		switch v := ex.(type) {
		case string:
			_ = json.Unmarshal([]byte(v), &m)
		case map[string]any:
			m = v
		}
		if m != nil {
			merged := map[string]any{}
			for k, v := range m {
				merged[k] = v
			}
			for k, v := range cfg {
				merged[k] = v
			}
			cfg = merged
		}
	}
	pick := func(names ...string) any {
		for _, n := range names {
			for _, k := range []string{n, "extra__google_cloud_platform__" + n} {
				if v, ok := cfg[k]; ok && v != nil && v != "" {
					return v
				}
			}
		}
		return nil
	}
	str := func(v any) string { s, _ := v.(string); return s }
	switch {
	case pick("credentials_json", "keyfile_dict") != nil:
		switch v := pick("credentials_json", "keyfile_dict").(type) {
		case string:
			raw = []byte(v)
		default:
			raw, _ = json.Marshal(v)
		}
	case str(pick("credentials_path", "key_path")) != "":
		path := str(pick("credentials_path", "key_path"))
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read credentials: %w", err)
		}
		raw = b
	case str(cfg["client_email"]) != "" && str(cfg["private_key"]) != "":
		raw, _ = json.Marshal(cfg)
	case os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "":
		b, err := os.ReadFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"))
		if err != nil {
			return nil, fmt.Errorf("read GOOGLE_APPLICATION_CREDENTIALS: %w", err)
		}
		raw = b
	default:
		return nil, nil
	}
	var k saKey
	if err := json.Unmarshal(raw, &k); err != nil {
		return nil, fmt.Errorf("credentials are not valid JSON: %w", err)
	}
	if k.ClientEmail == "" || k.PrivateKey == "" {
		return nil, errors.New("credentials must be a service account key (client_email and private_key)")
	}
	if k.TokenURI == "" {
		k.TokenURI = "https://oauth2.googleapis.com/token"
	}
	return &k, nil
}

func parseRSAKey(pemStr string) (*rsa.PrivateKey, error) {
	blk, _ := pem.Decode([]byte(pemStr))
	if blk == nil {
		return nil, errors.New("private_key is not PEM")
	}
	if k, err := x509.ParsePKCS8PrivateKey(blk.Bytes); err == nil {
		if rk, ok := k.(*rsa.PrivateKey); ok {
			return rk, nil
		}
		return nil, errors.New("private_key is not an RSA key")
	}
	return x509.ParsePKCS1PrivateKey(blk.Bytes)
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func signJWT(k *saKey, scope string, now time.Time) (string, error) {
	rk, err := parseRSAKey(k.PrivateKey)
	if err != nil {
		return "", err
	}
	hdr, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	cl, _ := json.Marshal(map[string]any{"iss": k.ClientEmail, "scope": scope, "aud": k.TokenURI,
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix()})
	in := b64(hdr) + "." + b64(cl)
	h := sha256.Sum256([]byte(in))
	sig, err := rsa.SignPKCS1v15(rand.Reader, rk, crypto.SHA256, h[:])
	if err != nil {
		return "", err
	}
	return in + "." + b64(sig), nil
}

// googleToken returns a cached access token for the connection and scope.
func googleToken(ctx context.Context, c Connection, scope string) (string, error) {
	k, err := googleCreds(c)
	if err != nil {
		return "", err
	}
	id := "metadata|" + scope
	if k != nil {
		id = k.ClientEmail + "|" + k.TokenURI + "|" + scope
	}
	tokMu.Lock()
	if t, ok := tokens[id]; ok && time.Now().Before(t.exp.Add(-time.Minute)) {
		tokMu.Unlock()
		return t.tok, nil
	}
	tokMu.Unlock()

	var tok string
	var ttl int
	if k == nil {
		tok, ttl, err = metadataToken(ctx, scope)
	} else {
		tok, ttl, err = exchangeJWT(ctx, k, scope)
	}
	if err != nil {
		return "", err
	}
	tokMu.Lock()
	tokens[id] = cachedToken{tok, time.Now().Add(time.Duration(ttl) * time.Second)}
	tokMu.Unlock()
	return tok, nil
}

func exchangeJWT(ctx context.Context, k *saKey, scope string) (string, int, error) {
	jwt, err := signJWT(k, scope, time.Now())
	if err != nil {
		return "", 0, fmt.Errorf("sign token request: %w", err)
	}
	form := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"}, "assertion": {jwt}}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, k.TokenURI, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := doJSON(req, &out); err != nil {
		return "", 0, fmt.Errorf("get access token: %w", err)
	}
	if out.AccessToken == "" {
		return "", 0, errors.New("get access token: empty response")
	}
	return out.AccessToken, out.ExpiresIn, nil
}

func metadataToken(ctx context.Context, scope string) (string, int, error) {
	cx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(cx, http.MethodGet, metadataURL+"?scopes="+url.QueryEscape(scope), nil)
	req.Header.Set("Metadata-Flavor", "Google")
	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := doJSON(req, &out); err != nil {
		return "", 0, errors.New("no Google credentials: paste the service account key JSON as the connection config, or set credentials_json / credentials_path (or GOOGLE_APPLICATION_CREDENTIALS); the GCE metadata server is not reachable")
	}
	return out.AccessToken, out.ExpiresIn, nil
}

var googleHTTP = &http.Client{Timeout: 90 * time.Second}

// doJSON runs the request and decodes a JSON answer; Google's error body is
// turned into the error message.
func doJSON(req *http.Request, out any) error { return doJSONWith(googleHTTP, req, out) }

func doJSONWith(c *http.Client, req *http.Request, out any) error {
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode/100 != 2 {
		var e struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
			Desc string `json:"error_description"`
		}
		_ = json.Unmarshal(b, &e)
		msg := firstNonEmpty(e.Error.Message, e.Desc, truncate(strings.TrimSpace(string(b)), 300))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, msg)
	}
	if out == nil || len(bytes.TrimSpace(b)) == 0 {
		return nil
	}
	return json.Unmarshal(b, out)
}

// googleCall sends an authorized JSON request.
func googleCall(ctx context.Context, c Connection, scope, method, u string, body, out any) error {
	tok, err := googleToken(ctx, c, scope)
	if err != nil {
		return err
	}
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return doJSON(req, out)
}

// googleConn resolves the connection (default google_cloud_default). A missing
// connection is fine: the credentials then come from the environment.
func googleConn(id string) (Connection, string) {
	id = firstNonEmpty(id, defaultGoogleCon)
	c, _ := GetConnection(id)
	if c.Config == nil {
		c.Config = map[string]any{}
	}
	return c, id
}
