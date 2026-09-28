package web

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"dagsflow-go/engine"
)

// Browser login: a stateless, HMAC-signed session cookie. The signing key
// comes from DAGSFLOW_SECRET_KEY or is generated once into the state dir, so
// several webserver replicas sharing DAGSFLOW_HOME accept the same cookies.
// Changing a user's password (or recreating the user) invalidates sessions.

const (
	sessionCookie = "dagsflow_session"
	sessionTTL    = 12 * time.Hour
)

func (s *Server) secret() ([]byte, error) {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	if s.key != nil {
		return s.key, nil
	}
	if k := os.Getenv("DAGSFLOW_SECRET_KEY"); k != "" {
		s.key = []byte(k)
		return s.key, nil
	}
	// generated once and shared through the metadata store (file or database),
	// so every webserver replica accepts the same cookies
	key, err := s.Store.SecretKey()
	if err != nil {
		return nil, err
	}
	s.key = key
	return key, nil
}

// loginLimiter slows down password guessing: after 5 failures for the same
// username or client IP within 15 minutes, logins are refused for a while
// (per webserver process).
type loginLimiter struct {
	mu   sync.Mutex
	hits map[string][]time.Time
}

const (
	loginWindow   = 15 * time.Minute
	loginMaxFails = 5
)

func (l *loginLimiter) prune(k string, now time.Time) []time.Time {
	kept := l.hits[k][:0]
	for _, t := range l.hits[k] {
		if now.Sub(t) < loginWindow {
			kept = append(kept, t)
		}
	}
	if len(kept) == 0 {
		delete(l.hits, k)
		return nil
	}
	l.hits[k] = kept
	return kept
}

// blocked returns how long the keys are locked out (0 = allowed).
func (l *loginLimiter) blocked(keys ...string) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	var wait time.Duration
	for _, k := range keys {
		if h := l.prune(k, now); len(h) >= loginMaxFails {
			if w := loginWindow - now.Sub(h[len(h)-loginMaxFails]); w > wait {
				wait = w
			}
		}
	}
	return wait
}

func (l *loginLimiter) fail(keys ...string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.hits == nil {
		l.hits = map[string][]time.Time{}
	}
	if len(l.hits) > 100000 { // bound memory under a flood
		l.hits = map[string][]time.Time{}
	}
	for _, k := range keys {
		l.hits[k] = append(l.hits[k], time.Now())
	}
}

func (l *loginLimiter) reset(keys ...string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, k := range keys {
		delete(l.hits, k)
	}
}

func userFingerprint(u *engine.User) string {
	h := sha256.Sum256([]byte(u.Username + "\x00" + u.PasswordHash))
	return hex.EncodeToString(h[:8])
}

func (s *Server) sign(payload string) (string, error) {
	key, err := s.secret()
	if err != nil {
		return "", err
	}
	m := hmac.New(sha256.New, key)
	m.Write([]byte(payload))
	return hex.EncodeToString(m.Sum(nil)), nil
}

func secureRequest(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func (s *Server) setSession(w http.ResponseWriter, r *http.Request, u *engine.User) error {
	exp := time.Now().Add(sessionTTL).Unix()
	payload := base64.RawURLEncoding.EncodeToString([]byte(u.Username)) + "." + strconv.FormatInt(exp, 10) + "." + userFingerprint(u)
	sig, err := s.sign(payload)
	if err != nil {
		return err
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: payload + "." + sig, Path: "/",
		MaxAge: int(sessionTTL.Seconds()), HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: secureRequest(r)})
	return nil
}

func clearSession(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: secureRequest(r)})
}

func (s *Server) sessionUser(r *http.Request) (*engine.User, bool) {
	c, err := r.Cookie(sessionCookie)
	if err != nil || c.Value == "" {
		return nil, false
	}
	parts := strings.Split(c.Value, ".")
	if len(parts) != 4 {
		return nil, false
	}
	payload := strings.Join(parts[:3], ".")
	want, err := s.sign(payload)
	if err != nil || subtle.ConstantTimeCompare([]byte(want), []byte(parts[3])) != 1 {
		return nil, false
	}
	exp, _ := strconv.ParseInt(parts[1], 10, 64)
	if time.Now().Unix() > exp {
		return nil, false
	}
	name, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, false
	}
	users, err := s.Store.Users()
	if err != nil {
		return nil, false
	}
	u := users[string(name)]
	if u == nil || !u.Active() || userFingerprint(u) != parts[2] {
		return nil, false
	}
	return u, true
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Dagsflow") != "1" {
		fail(w, http.StatusForbidden, "missing X-Dagsflow header")
		return
	}
	users, _ := s.Store.Users()
	if len(users) == 0 {
		writeJSON(w, 200, map[string]any{"username": "anonymous", "role": engine.RoleAdmin, "auth_enabled": false})
		return
	}
	var body struct{ Username, Password string }
	if err := readBody(r, &body); err != nil {
		fail(w, 400, "invalid body")
		return
	}
	name := strings.TrimSpace(body.Username)
	keys := []string{"u:" + strings.ToLower(name), "ip:" + clientIP(r)}
	if wait := s.limiter.blocked(keys...); wait > 0 {
		s.Store.Audit(engine.AuditEvent{User: name, Event: "login_blocked", Extra: clientIP(r)})
		w.Header().Set("Retry-After", fmt.Sprint(int(wait.Seconds())+1))
		fail(w, http.StatusTooManyRequests, "Too many failed logins. Try again in %s.", wait.Round(time.Minute))
		return
	}
	u, ok := s.Store.Authenticate(name, body.Password, "")
	if !ok {
		s.limiter.fail(keys...)
		s.Store.Audit(engine.AuditEvent{User: name, Event: "login_failed", Extra: clientIP(r)})
		time.Sleep(400 * time.Millisecond) // slow down guessing
		fail(w, http.StatusUnauthorized, "Invalid username or password")
		return
	}
	s.limiter.reset(keys...)
	s.Store.RecordLogin(u.Username)
	s.Store.Audit(engine.AuditEvent{User: u.Username, Event: "login", Extra: clientIP(r)})
	if err := s.setSession(w, r, u); err != nil {
		fail(w, 500, "%v", err)
		return
	}
	writeJSON(w, 200, map[string]any{"username": u.Username, "role": u.Role, "auth_enabled": true})
}

func clientIP(r *http.Request) string {
	// X-Forwarded-For is only trusted behind a proxy that sets it
	// (DAGSFLOW_TRUST_PROXY_HEADERS=true), otherwise clients could spoof it
	if f := r.Header.Get("X-Forwarded-For"); f != "" && trustProxy() {
		return strings.TrimSpace(strings.Split(f, ",")[0])
	}
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i > 0 {
		host = host[:i]
	}
	return host
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if u, ok := s.sessionUser(r); ok {
		s.Store.Audit(engine.AuditEvent{User: u.Username, Event: "logout"})
	}
	clearSession(w, r)
	w.WriteHeader(http.StatusNoContent)
}

func trustProxy() bool {
	v := strings.ToLower(os.Getenv("DAGSFLOW_TRUST_PROXY_HEADERS"))
	return v == "1" || v == "true" || v == "yes"
}
