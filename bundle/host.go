package bundle

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"

	"dagsflow-go/dag"
)

// host is the scheduler-side RPC server that bundle processes call back into.
// Every invocation gets a random token bound to its TaskContext.
type host struct {
	addr string
	mu   sync.Mutex
	sess map[string]*session
}

type session struct {
	tctx   *dag.TaskContext // nil for callbacks
	mu     sync.Mutex
	result Result
}

var (
	hostOnce sync.Once
	theHost  *host
	hostErr  error
)

func getHost() (*host, error) {
	hostOnce.Do(func() {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			hostErr = err
			return
		}
		h := &host{addr: ln.Addr().String(), sess: map[string]*session{}}
		srv := rpc.NewServer()
		if err := srv.RegisterName("Host", &Host{h}); err != nil {
			hostErr = err
			return
		}
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go srv.ServeCodec(jsonrpc.NewServerCodec(c))
			}
		}()
		theHost = h
	})
	return theHost, hostErr
}

func (h *host) open(tctx *dag.TaskContext) (string, *session) {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	tok := hex.EncodeToString(b)
	s := &session{tctx: tctx}
	h.mu.Lock()
	h.sess[tok] = s
	h.mu.Unlock()
	return tok, s
}

func (h *host) close(tok string) {
	h.mu.Lock()
	delete(h.sess, tok)
	h.mu.Unlock()
}

func (h *host) get(tok string) (*session, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.sess[tok]
	if !ok {
		return nil, errors.New("invalid or expired token")
	}
	return s, nil
}

// Host is the RPC service exposed to bundle processes.
type Host struct{ h *host }

func (x *Host) task(a *Args) (*dag.TaskContext, error) {
	s, err := x.h.get(a.Token)
	if err != nil {
		return nil, err
	}
	if s.tctx == nil {
		return nil, errors.New("not available in callbacks")
	}
	return s.tctx, nil
}

func (x *Host) XComPush(a *Args, r *Reply) error {
	t, err := x.task(a)
	if err != nil {
		return err
	}
	return t.XComPush(a.Key, a.Value)
}

func (x *Host) XComPull(a *Args, r *Reply) error {
	t, err := x.task(a)
	if err != nil {
		return err
	}
	r.Value, r.Found = t.XComLookup(a.TaskID, a.Key)
	return nil
}

func (x *Host) TriggerDagRun(a *Args, r *Reply) error {
	t, err := x.task(a)
	if err != nil {
		return err
	}
	r.Str, err = t.TriggerDagRun(a.DagID, a.Conf, a.Wait)
	return err
}

func (x *Host) ExternalTaskState(a *Args, r *Reply) error {
	t, err := x.task(a)
	if err != nil {
		return err
	}
	st, found, err := t.ExternalTaskState(a.DagID, a.TaskID, a.Logical)
	r.Str, r.Found = string(st), found
	return err
}

func (x *Host) SetRendered(a *Args, r *Reply) error {
	t, err := x.task(a)
	if err != nil {
		return err
	}
	s, _ := a.Value.(string)
	t.RecordRendered(a.Name, s)
	return nil
}

func (x *Host) GetVariable(a *Args, r *Reply) error {
	if _, err := x.h.get(a.Token); err != nil {
		return err
	}
	v, err := dag.GetVariable(a.Name)
	r.Str, r.Found = v, err == nil
	return nil
}

func (x *Host) GetConnection(a *Args, r *Reply) error {
	if _, err := x.h.get(a.Token); err != nil {
		return err
	}
	c, ok := dag.GetConnection(a.Name)
	if ok {
		r.Conn, r.Found = &c, true
	}
	return nil
}

func (x *Host) Report(a *Args, r *Reply) error {
	s, err := x.h.get(a.Token)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.result = a.Result
	s.mu.Unlock()
	return nil
}
