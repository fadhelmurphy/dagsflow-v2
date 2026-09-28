package dag

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// CallbackContext is passed to on_success / on_failure / on_retry / sla_miss callbacks.
type CallbackContext struct {
	Event       string // task_success, task_failure, task_retry, dagrun_success, dagrun_failure, sla_miss
	DagID       string
	RunID       string
	TaskID      string // empty for DagRun callbacks
	MapIndex    int
	State       State
	TryNumber   int
	LogicalDate time.Time
	Err         error
	LogPath     string
}

// URL returns a link to the run in the web UI (needs DAGSFLOW_BASE_URL).
func (c CallbackContext) URL() string {
	base := strings.TrimRight(os.Getenv("DAGSFLOW_BASE_URL"), "/")
	if base == "" {
		return ""
	}
	return fmt.Sprintf("%s/#/dags/%s?tab=grid&run=%s", base, url.PathEscape(c.DagID), url.QueryEscape(c.RunID))
}

// Callback is called asynchronously by the engine; errors are logged, never fatal.
type Callback func(ctx context.Context, cc CallbackContext) error

// RunCallbacks runs callbacks with a timeout and panic recovery.
func RunCallbacks(cbs []Callback, cc CallbackContext, logf func(string, ...any)) {
	for _, cb := range cbs {
		func() {
			defer func() {
				if p := recover(); p != nil {
					logf("callback %s panicked: %v", cc.Event, p)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := cb(ctx, cc); err != nil {
				logf("callback %s failed: %v", cc.Event, err)
			}
		}()
	}
}

// GoogleChatNotifier posts a Card v2 message to the webhook stored in connection
// connID (type google_chat, Config.webhook_url). Resolved at call time, so the
// webhook can live in a secrets backend.
func GoogleChatNotifier(connID string) Callback {
	return func(ctx context.Context, cc CallbackContext) error {
		conn, ok := GetConnection(connID)
		if !ok {
			return fmt.Errorf("connection %q not found", connID)
		}
		hook := conn.String("webhook_url")
		if hook == "" {
			return fmt.Errorf("connection %q has no webhook_url", connID)
		}
		return postGoogleChat(ctx, hook, cc)
	}
}

// GoogleChatWebhook is GoogleChatNotifier with an inline URL.
func GoogleChatWebhook(webhookURL string) Callback {
	return func(ctx context.Context, cc CallbackContext) error { return postGoogleChat(ctx, webhookURL, cc) }
}

func postGoogleChat(ctx context.Context, hook string, cc CallbackContext) error {
	icons := map[string]string{"task_failure": "❌", "dagrun_failure": "❌", "task_retry": "🔁",
		"sla_miss": "⏰", "task_success": "✅", "dagrun_success": "✅"}
	subject := cc.DagID
	if cc.TaskID != "" {
		subject += "." + cc.TaskID
		if cc.MapIndex >= 0 {
			subject += fmt.Sprintf("[%d]", cc.MapIndex)
		}
	}
	kv := func(label, text string) map[string]any {
		return map[string]any{"decoratedText": map[string]any{"topLabel": label, "text": html.EscapeString(text)}}
	}
	widgets := []any{kv("Run", cc.RunID), kv("Logical date", cc.LogicalDate.Format(time.RFC3339))}
	if cc.TaskID != "" {
		widgets = append(widgets, kv("Try", fmt.Sprint(cc.TryNumber)))
	}
	if cc.Err != nil {
		msg := cc.Err.Error()
		if len(msg) > 1500 {
			msg = msg[:1500] + "…"
		}
		widgets = append(widgets, map[string]any{"textParagraph": map[string]any{"text": "<b>Error</b><br>" + html.EscapeString(msg)}})
	}
	if u := cc.URL(); u != "" {
		widgets = append(widgets, map[string]any{"buttonList": map[string]any{"buttons": []any{
			map[string]any{"text": "Open in dagsflow", "onClick": map[string]any{"openLink": map[string]any{"url": u}}}}}})
	}
	body := map[string]any{"cardsV2": []any{map[string]any{
		"cardId": "dagsflow",
		"card": map[string]any{
			"header":   map[string]any{"title": fmt.Sprintf("%s %s", icons[cc.Event], strings.ReplaceAll(cc.Event, "_", " ")), "subtitle": subject},
			"sections": []any{map[string]any{"widgets": widgets}},
		},
	}}}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hook, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("google chat returned %s", resp.Status)
	}
	return nil
}
