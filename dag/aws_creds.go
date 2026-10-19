package dag

import (
	"context"
	"encoding/json"
	"encoding/xml"
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

// AWS credentials without static keys, in the order the AWS SDKs use:
//
//  1. access_key_id / secret_access_key of the connection, or AWS_ACCESS_KEY_ID
//     / AWS_SECRET_ACCESS_KEY (+ session token)
//  2. web identity (EKS IRSA): AWS_WEB_IDENTITY_TOKEN_FILE + AWS_ROLE_ARN →
//     STS AssumeRoleWithWebIdentity
//  3. container credentials (ECS task role, EKS Pod Identity):
//     AWS_CONTAINER_CREDENTIALS_RELATIVE_URI or _FULL_URI (+ authorization token)
//  4. EC2 instance profile through IMDSv2 (AWS_EC2_METADATA_DISABLED=true skips it)
//
// A role_arn on the connection is then assumed with STS AssumeRole (cross
// account access), with optional external_id. Temporary credentials are cached
// until five minutes before they expire.

var awsHTTP = &http.Client{Timeout: 10 * time.Second}

type cachedCreds struct {
	cr      awsCreds
	expires time.Time
}

var (
	awsCacheMu sync.Mutex
	awsCache   = map[string]cachedCreds{}
)

func awsCached(key string, fetch func() (awsCreds, time.Time, error)) (awsCreds, error) {
	awsCacheMu.Lock()
	c, ok := awsCache[key]
	awsCacheMu.Unlock()
	if ok && time.Until(c.expires) > 5*time.Minute {
		return c.cr, nil
	}
	cr, exp, err := fetch()
	if err != nil {
		return cr, err
	}
	if exp.IsZero() {
		exp = time.Now().Add(time.Hour)
	}
	awsCacheMu.Lock()
	awsCache[key] = cachedCreds{cr, exp}
	awsCacheMu.Unlock()
	return cr, nil
}

// resetAWSCache is for tests.
func resetAWSCache() {
	awsCacheMu.Lock()
	awsCache = map[string]cachedCreds{}
	awsCacheMu.Unlock()
}

type wireCreds struct {
	AccessKeyID     string `json:"AccessKeyId" xml:"AccessKeyId"`
	SecretAccessKey string `json:"SecretAccessKey" xml:"SecretAccessKey"`
	Token           string `json:"Token"`
	SessionToken    string `xml:"SessionToken"`
	Expiration      string `json:"Expiration" xml:"Expiration"`
}

func (w wireCreds) creds() (awsCreds, time.Time, error) {
	if w.AccessKeyID == "" || w.SecretAccessKey == "" {
		return awsCreds{}, time.Time{}, errors.New("response holds no credentials")
	}
	exp, _ := time.Parse(time.RFC3339, w.Expiration)
	return awsCreds{Key: w.AccessKeyID, Secret: w.SecretAccessKey, Token: firstNonEmpty(w.Token, w.SessionToken)}, exp, nil
}

func envTrim(k string) string { return strings.TrimSpace(os.Getenv(k)) }

func readSecretFile(p string) (string, error) {
	b, err := os.ReadFile(p)
	return strings.TrimSpace(string(b)), err
}

// ---- STS ----------------------------------------------------------------------------

func stsEndpoint(region string) string {
	if e := firstNonEmpty(envTrim("AWS_ENDPOINT_URL_STS"), envTrim("AWS_STS_ENDPOINT")); e != "" {
		return strings.TrimRight(e, "/") + "/"
	}
	if region == "" {
		return "https://sts.amazonaws.com/"
	}
	return "https://sts." + region + ".amazonaws.com/"
}

type stsResponse struct {
	Result struct {
		Credentials wireCreds `xml:"Credentials"`
	} `xml:",any"`
}

func stsCall(ctx context.Context, region string, form url.Values, signWith *awsCreds) (awsCreds, time.Time, error) {
	form.Set("Version", "2011-06-15")
	body := form.Encode()
	ep := stsEndpoint(region)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep, strings.NewReader(body))
	if err != nil {
		return awsCreds{}, time.Time{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if signWith != nil {
		u, _ := url.Parse(ep)
		now := time.Now().UTC()
		ph := sha256Hex([]byte(body))
		hdr := map[string]string{"host": u.Host, "content-type": "application/x-www-form-urlencoded", "x-amz-date": now.Format("20060102T150405Z")}
		if signWith.Token != "" {
			hdr["x-amz-security-token"] = signWith.Token
		}
		auth := signV4(http.MethodPost, firstNonEmpty(u.Path, "/"), "", hdr, ph, firstNonEmpty(region, "us-east-1"), "sts", *signWith, now)
		for k, v := range hdr {
			if k != "host" {
				req.Header.Set(k, v)
			}
		}
		req.Header.Set("Authorization", auth)
	}
	resp, err := awsHTTP.Do(req)
	if err != nil {
		return awsCreds{}, time.Time{}, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		msg := string(b)
		if i := strings.Index(msg, "<Message>"); i >= 0 {
			if j := strings.Index(msg[i:], "</Message>"); j > 0 {
				msg = msg[i+9 : i+j]
			}
		}
		return awsCreds{}, time.Time{}, fmt.Errorf("STS %s: HTTP %d: %s", form.Get("Action"), resp.StatusCode, truncate(strings.TrimSpace(msg), 300))
	}
	var out stsResponse
	if err := xml.Unmarshal(b, &out); err != nil {
		return awsCreds{}, time.Time{}, fmt.Errorf("STS %s: %w", form.Get("Action"), err)
	}
	return out.Result.Credentials.creds()
}

func sessionName() string {
	h, _ := os.Hostname()
	n := "dagsflow-" + h
	if len(n) > 64 {
		n = n[:64]
	}
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("=,.@-_", r) {
			return r
		}
		return '-'
	}, n)
}

// ---- providers ----------------------------------------------------------------------

func webIdentityCreds(ctx context.Context, region string) (awsCreds, bool, error) {
	tokFile, role := envTrim("AWS_WEB_IDENTITY_TOKEN_FILE"), envTrim("AWS_ROLE_ARN")
	if tokFile == "" || role == "" {
		return awsCreds{}, false, nil
	}
	cr, err := awsCached("web:"+role, func() (awsCreds, time.Time, error) {
		tok, err := readSecretFile(tokFile)
		if err != nil {
			return awsCreds{}, time.Time{}, fmt.Errorf("AWS_WEB_IDENTITY_TOKEN_FILE: %w", err)
		}
		f := url.Values{"Action": {"AssumeRoleWithWebIdentity"}, "RoleArn": {role},
			"RoleSessionName": {firstNonEmpty(envTrim("AWS_ROLE_SESSION_NAME"), sessionName())}, "WebIdentityToken": {tok}}
		return stsCall(ctx, region, f, nil)
	})
	return cr, true, err
}

func containerCreds(ctx context.Context) (awsCreds, bool, error) {
	rel, full := envTrim("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"), envTrim("AWS_CONTAINER_CREDENTIALS_FULL_URI")
	if rel == "" && full == "" {
		return awsCreds{}, false, nil
	}
	u := full
	if rel != "" {
		u = "http://169.254.170.2" + rel
	}
	cr, err := awsCached("container:"+u, func() (awsCreds, time.Time, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return awsCreds{}, time.Time{}, err
		}
		tok := envTrim("AWS_CONTAINER_AUTHORIZATION_TOKEN")
		if f := envTrim("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"); f != "" {
			if tok, err = readSecretFile(f); err != nil {
				return awsCreds{}, time.Time{}, fmt.Errorf("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE: %w", err)
			}
		}
		if tok != "" {
			req.Header.Set("Authorization", tok)
		}
		var w wireCreds
		if err := getJSON(req, &w); err != nil {
			return awsCreds{}, time.Time{}, fmt.Errorf("container credentials: %w", err)
		}
		return w.creds()
	})
	return cr, true, err
}

func imdsBase() string {
	return strings.TrimRight(firstNonEmpty(envTrim("AWS_EC2_METADATA_SERVICE_ENDPOINT"), "http://169.254.169.254"), "/")
}

// imdsGet reads one IMDSv2 path (fetching a session token first).
func imdsGet(ctx context.Context, path string) (string, error) {
	c := &http.Client{Timeout: 2 * time.Second}
	treq, err := http.NewRequestWithContext(ctx, http.MethodPut, imdsBase()+"/latest/api/token", nil)
	if err != nil {
		return "", err
	}
	treq.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "21600")
	tresp, err := c.Do(treq)
	if err != nil {
		return "", err
	}
	tb, _ := io.ReadAll(io.LimitReader(tresp.Body, 4096))
	tresp.Body.Close()
	if tresp.StatusCode != 200 {
		return "", fmt.Errorf("IMDS token: HTTP %d", tresp.StatusCode)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, imdsBase()+path, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-aws-ec2-metadata-token", strings.TrimSpace(string(tb)))
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("IMDS %s: HTTP %d", path, resp.StatusCode)
	}
	return strings.TrimSpace(string(b)), nil
}

func imdsCreds(ctx context.Context) (awsCreds, bool, error) {
	if strings.EqualFold(envTrim("AWS_EC2_METADATA_DISABLED"), "true") {
		return awsCreds{}, false, nil
	}
	cr, err := awsCached("imds", func() (awsCreds, time.Time, error) {
		role, err := imdsGet(ctx, "/latest/meta-data/iam/security-credentials/")
		if err != nil {
			return awsCreds{}, time.Time{}, err
		}
		role = strings.TrimSpace(strings.SplitN(role, "\n", 2)[0])
		if role == "" {
			return awsCreds{}, time.Time{}, errors.New("the instance has no IAM role")
		}
		body, err := imdsGet(ctx, "/latest/meta-data/iam/security-credentials/"+url.PathEscape(role))
		if err != nil {
			return awsCreds{}, time.Time{}, err
		}
		var w wireCreds
		if err := json.Unmarshal([]byte(body), &w); err != nil {
			return awsCreds{}, time.Time{}, err
		}
		cr, exp, err := w.creds()
		if err == nil {
			if r, rerr := imdsGet(ctx, "/latest/meta-data/placement/region"); rerr == nil {
				cr.Region = r
			}
		}
		return cr, exp, err
	})
	if err != nil {
		return cr, false, err // not on EC2 (or no role): not an error by itself
	}
	return cr, true, nil
}

func getJSON(req *http.Request, v any) error {
	resp, err := awsHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(strings.TrimSpace(string(b)), 200))
	}
	return json.Unmarshal(b, v)
}

// awsCredentialsCtx resolves the credentials of a connection through the chain.
func awsCredentialsCtx(ctx context.Context, connID string) (awsCreds, error) {
	id := firstNonEmpty(connID, defaultAWSCon)
	c, _ := GetConnection(id)
	get := func(key string, env ...string) string {
		if v := strings.TrimSpace(c.String(key)); v != "" {
			return v
		}
		for _, e := range env {
			if v := envTrim(e); v != "" {
				return v
			}
		}
		return ""
	}
	region := get("region", "AWS_REGION", "AWS_DEFAULT_REGION")
	endpoint := get("endpoint_url", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL")
	cr := awsCreds{Key: get("access_key_id", "AWS_ACCESS_KEY_ID"), Secret: get("secret_access_key", "AWS_SECRET_ACCESS_KEY"),
		Token: get("session_token", "AWS_SESSION_TOKEN")}
	source := "keys"
	if cr.Key == "" || cr.Secret == "" {
		var tried []string
		found := false
		for _, p := range []struct {
			name string
			fn   func() (awsCreds, bool, error)
		}{
			{"web identity", func() (awsCreds, bool, error) { return webIdentityCreds(ctx, region) }},
			{"container", func() (awsCreds, bool, error) { return containerCreds(ctx) }},
			{"instance profile", func() (awsCreds, bool, error) { return imdsCreds(ctx) }},
		} {
			got, ok, err := p.fn()
			if err != nil && ok {
				return awsCreds{}, fmt.Errorf("AWS %s credentials: %w", p.name, err)
			}
			if ok {
				cr, source, found = got, p.name, true
				break
			}
			tried = append(tried, p.name)
		}
		if !found {
			return awsCreds{}, fmt.Errorf("no AWS credentials: add access_key_id and secret_access_key to the %q connection, set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, or run with an IAM role (tried %s)", id, strings.Join(tried, ", "))
		}
		if region == "" {
			region = cr.Region
		}
	}
	if role := strings.TrimSpace(c.String("role_arn")); role != "" {
		base := cr
		assumed, err := awsCached("assume:"+role+":"+base.Key, func() (awsCreds, time.Time, error) {
			f := url.Values{"Action": {"AssumeRole"}, "RoleArn": {role}, "RoleSessionName": {sessionName()}}
			if ext := strings.TrimSpace(c.String("external_id")); ext != "" {
				f.Set("ExternalId", ext)
			}
			return stsCall(ctx, region, f, &base)
		})
		if err != nil {
			return awsCreds{}, fmt.Errorf("assume role %s (with %s credentials): %w", role, source, err)
		}
		cr = assumed
	}
	cr.Region, cr.Endpoint = region, endpoint
	return cr, nil
}
