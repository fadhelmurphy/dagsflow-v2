package web

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	p := filepath.Join(s.Store.Home, "state", "webserver_secret_key")
	if b, err := os.ReadFile(p); err == nil && len(b) >= 32 {
		s.key = b
		return b, nil
	}
	raw := make([]byte, 32)
	_, _ = rand.Read(raw)
	key := []byte(hex.EncodeToString(raw))
	_ = os.MkdirAll(filepath.Dir(p), 0o755)
	// O_EXCL: if another replica created it first, use theirs
	f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err == nil {
		_, err = f.Write(key)
		f.Close()
		if err != nil {
			return nil, err
		}
		s.key = key
		return key, nil
	}
	if b, err := os.ReadFile(p); err == nil && len(b) >= 32 {
		s.key = b
		return b, nil
	}
	return nil, errors.New("cannot create webserver secret key")
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
	if u == nil || userFingerprint(u) != parts[2] {
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
	u, ok := s.Store.Authenticate(strings.TrimSpace(body.Username), body.Password, "")
	if !ok {
		time.Sleep(400 * time.Millisecond) // slow down guessing
		fail(w, http.StatusUnauthorized, "Invalid username or password")
		return
	}
	if err := s.setSession(w, r, u); err != nil {
		fail(w, 500, "%v", err)
		return
	}
	writeJSON(w, 200, map[string]any{"username": u.Username, "role": u.Role, "auth_enabled": true})
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	clearSession(w, r)
	w.WriteHeader(http.StatusNoContent)
}
