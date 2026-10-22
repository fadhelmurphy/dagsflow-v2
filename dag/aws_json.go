package dag

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// AWS JSON 1.1 APIs (Athena, Redshift Data API): a signed POST with an
// X-Amz-Target header. Plain REST, no SDK.

// awsServiceURL is the endpoint of a service (tests point it at a fake server).
var awsServiceURL = func(service, region string) string {
	return "https://" + service + "." + region + ".amazonaws.com/"
}

var awsAPIHTTP = &http.Client{Timeout: 2 * time.Minute}

// awsJSON calls target (e.g. "AmazonAthena.StartQueryExecution") of service
// ("athena", "redshift-data") and decodes the answer into out.
func awsJSON(ctx context.Context, cr awsCreds, service, target string, body, out any) error {
	if cr.Region == "" {
		return fmt.Errorf("no AWS region: set region on the connection or AWS_REGION")
	}
	u, err := url.Parse(awsServiceURL(service, cr.Region))
	if err != nil {
		return err
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	ph := sha256Hex(payload)
	hdr := map[string]string{"host": u.Host, "content-type": "application/x-amz-json-1.1", "x-amz-target": target,
		"x-amz-date": now.Format("20060102T150405Z"), "x-amz-content-sha256": ph}
	if cr.Token != "" {
		hdr["x-amz-security-token"] = cr.Token
	}
	path := u.Path
	if path == "" {
		path = "/"
	}
	auth := signV4(http.MethodPost, path, "", hdr, ph, cr.Region, service, cr, now)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	for k, v := range hdr {
		if k != "host" {
			req.Header.Set(k, v)
		}
	}
	req.Header.Set("Authorization", auth)
	resp, err := awsAPIHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode/100 != 2 {
		var e struct {
			Type    string `json:"__type"`
			Message string `json:"message"`
			Msg2    string `json:"Message"`
		}
		_ = json.Unmarshal(b, &e)
		typ := e.Type
		if i := strings.LastIndex(typ, "#"); i >= 0 {
			typ = typ[i+1:]
		}
		msg := firstNonEmpty(e.Message, e.Msg2, truncate(strings.TrimSpace(string(b)), 300))
		if typ != "" {
			msg = typ + ": " + msg
		}
		return fmt.Errorf("%s HTTP %d: %s", target, resp.StatusCode, msg)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(b, out)
}

// waitFor polls check every interval (growing to 5 s) until it says done.
func waitFor(ctx context.Context, check func() (bool, error)) error {
	d := 300 * time.Millisecond
	for {
		done, err := check()
		if err != nil || done {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
		}
		if d < 5*time.Second {
			d = d * 3 / 2
		}
	}
}
