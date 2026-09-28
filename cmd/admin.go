package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"time"

	"dagsflow-go/dag"
	"dagsflow-go/engine"

	"github.com/spf13/cobra"
)

func adminCmds() []*cobra.Command {
	return []*cobra.Command{variablesCmd(), connectionsCmd(), poolsCmd(), usersCmd(), datasetsCmd(), configCmd()}
}

func warnPlaintext() {
	if !engine.EncryptionEnabled() {
		fmt.Fprintln(os.Stderr, "warning: DAGSFLOW_FERNET_KEY is not set, value stored in plain text (see `config gen-key`)")
	}
}

// ---- variables -----------------------------------------------------------------

func variablesCmd() *cobra.Command {
	c := &cobra.Command{Use: "variables", Short: "Manage variables (Variable.get / {{ var \"key\" }})"}
	c.AddCommand(
		&cobra.Command{Use: "list", Short: "List variable keys", Run: func(*cobra.Command, []string) {
			vars, err := newStore().Variables()
			if err != nil {
				exitf("%v", err)
			}
			w := table()
			fmt.Fprintln(w, "KEY\tENCRYPTED\tUPDATED\tDESCRIPTION")
			for _, v := range vars {
				fmt.Fprintf(w, "%s\t%v\t%s\t%s\n", v.Key, v.Secret.Encrypted, v.UpdatedAt.Local().Format(time.DateTime), v.Description)
			}
			w.Flush()
		}},
		&cobra.Command{Use: "get <key>", Short: "Print a variable (resolved through all secrets backends)", Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, a []string) {
				v, err := dag.GetVariable(a[0])
				if err != nil {
					exitf("%v", err)
				}
				fmt.Println(v)
			}},
		func() *cobra.Command {
			sc := &cobra.Command{Use: "set <key> <value>", Short: "Set a variable", Args: cobra.ExactArgs(2),
				Run: func(cmd *cobra.Command, a []string) {
					desc, _ := cmd.Flags().GetString("description")
					warnPlaintext()
					if err := newStore().SetVariable(a[0], a[1], desc); err != nil {
						exitf("%v", err)
					}
					fmt.Println("variable set:", a[0])
				}}
			sc.Flags().String("description", "", "description")
			return sc
		}(),
		&cobra.Command{Use: "delete <key>", Short: "Delete a variable", Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, a []string) {
				if err := newStore().DeleteVariable(a[0]); err != nil {
					exitf("%v", err)
				}
				fmt.Println("variable deleted:", a[0])
			}},
		&cobra.Command{Use: "import <file.json>", Short: "Import {\"key\": value, ...}", Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, a []string) {
				b, err := os.ReadFile(a[0])
				if err != nil {
					exitf("%v", err)
				}
				var m map[string]any
				if err := json.Unmarshal(b, &m); err != nil {
					exitf("%v", err)
				}
				warnPlaintext()
				st := newStore()
				for k, v := range m {
					s, ok := v.(string)
					if !ok {
						raw, _ := json.Marshal(v)
						s = string(raw)
					}
					if err := st.SetVariable(k, s, ""); err != nil {
						exitf("%s: %v", k, err)
					}
				}
				fmt.Printf("imported %d variables\n", len(m))
			}},
	)
	return c
}

// ---- connections ---------------------------------------------------------------

func connectionsCmd() *cobra.Command {
	c := &cobra.Command{Use: "connections", Short: "Manage connections stored in the metastore"}
	add := &cobra.Command{Use: "add <conn_id>", Short: "Add or replace a connection", Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, a []string) {
			typ, _ := cmd.Flags().GetString("conn-type")
			cfg, _ := cmd.Flags().GetString("config")
			desc, _ := cmd.Flags().GetString("description")
			conn := dag.Connection{Type: typ, Config: map[string]any{}}
			if cfg != "" {
				if err := json.Unmarshal([]byte(cfg), &conn.Config); err != nil {
					exitf("--config must be a JSON object: %v", err)
				}
			}
			warnPlaintext()
			if err := newStore().SetConnection(a[0], conn, desc); err != nil {
				exitf("%v", err)
			}
			fmt.Println("connection saved:", a[0])
		}}
	add.Flags().String("conn-type", "", "e.g. google_cloud_platform, google_chat, http")
	add.Flags().String("config", "", `JSON, e.g. '{"webhook_url":"https://chat.googleapis.com/..."}'`)
	add.Flags().String("description", "", "description")
	_ = add.MarkFlagRequired("conn-type")

	c.AddCommand(
		&cobra.Command{Use: "list", Short: "List connections (values hidden)", Run: func(*cobra.Command, []string) {
			conns, err := newStore().Connections()
			if err != nil {
				exitf("%v", err)
			}
			w := table()
			fmt.Fprintln(w, "CONN_ID\tTYPE\tENCRYPTED\tUPDATED\tDESCRIPTION")
			for _, c := range conns {
				fmt.Fprintf(w, "%s\t%s\t%v\t%s\t%s\n", c.ID, c.Type, c.Secret.Encrypted, c.UpdatedAt.Local().Format(time.DateTime), c.Description)
			}
			w.Flush()
		}},
		&cobra.Command{Use: "get <conn_id>", Short: "Show a connection (resolved through all backends)", Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, a []string) {
				conn, ok := dag.GetConnection(a[0])
				if !ok {
					exitf("connection %s not found", a[0])
				}
				b, _ := json.MarshalIndent(conn, "", "  ")
				fmt.Println(string(b))
			}},
		add,
		&cobra.Command{Use: "delete <conn_id>", Short: "Delete a connection", Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, a []string) {
				if err := newStore().DeleteConnection(a[0]); err != nil {
					exitf("%v", err)
				}
				fmt.Println("connection deleted:", a[0])
			}},
	)
	return c
}

// ---- pools ------------------------------------------------------------------------

func poolsCmd() *cobra.Command {
	c := &cobra.Command{Use: "pools", Short: "Manage pools (limit concurrency per resource)"}
	set := &cobra.Command{Use: "set <name> <slots>", Short: "Create or resize a pool", Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, a []string) {
			var n int
			if _, err := fmt.Sscan(a[1], &n); err != nil {
				exitf("slots must be a number")
			}
			desc, _ := cmd.Flags().GetString("description")
			if err := newStore().SetPool(a[0], n, desc); err != nil {
				exitf("%v", err)
			}
			fmt.Printf("pool %s: %d slots\n", a[0], n)
		}}
	set.Flags().String("description", "", "description")
	c.AddCommand(
		&cobra.Command{Use: "list", Short: "List pools", Run: func(*cobra.Command, []string) {
			pools, err := newStore().Pools()
			if err != nil {
				exitf("%v", err)
			}
			names := make([]string, 0, len(pools))
			for n := range pools {
				names = append(names, n)
			}
			sort.Strings(names)
			w := table()
			fmt.Fprintln(w, "POOL\tSLOTS\tDESCRIPTION")
			for _, n := range names {
				fmt.Fprintf(w, "%s\t%d\t%s\n", n, pools[n].Slots, pools[n].Description)
			}
			w.Flush()
		}},
		set,
		&cobra.Command{Use: "delete <name>", Short: "Delete a pool", Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, a []string) {
				if err := newStore().DeletePool(a[0]); err != nil {
					exitf("%v", err)
				}
				fmt.Println("pool deleted:", a[0])
			}},
	)
	return c
}

// ---- users --------------------------------------------------------------------------

func readPassword() string {
	if pw := os.Getenv("DAGSFLOW_PASSWORD"); pw != "" {
		return pw
	}
	fmt.Fprint(os.Stderr, "Password: ")
	if runtime.GOOS != "windows" { // hide input on a terminal (best effort)
		off := exec.Command("stty", "-echo")
		off.Stdin = os.Stdin
		if off.Run() == nil {
			defer func() {
				on := exec.Command("stty", "echo")
				on.Stdin = os.Stdin
				_ = on.Run()
				fmt.Fprintln(os.Stderr)
			}()
		}
	}
	line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	return strings.TrimSpace(line)
}

func usersCmd() *cobra.Command {
	c := &cobra.Command{Use: "users", Short: "Manage web/API users and roles (Viewer, User, Op, Admin)"}
	create := &cobra.Command{Use: "create", Short: "Create a user (password from prompt or DAGSFLOW_PASSWORD)",
		Run: func(cmd *cobra.Command, _ []string) {
			u, _ := cmd.Flags().GetString("username")
			r, _ := cmd.Flags().GetString("role")
			role, err := engine.ParseRole(r)
			if err != nil {
				exitf("%v", err)
			}
			if ok, _ := cmd.Flags().GetBool("exists-ok"); ok {
				if users, _ := newStore().Users(); users[u] != nil {
					fmt.Printf("user %s already exists\n", u)
					return
				}
			}
			if err := newStore().CreateUser(u, readPassword(), role); err != nil {
				exitf("%v", err)
			}
			fn, _ := cmd.Flags().GetString("first-name")
			ln, _ := cmd.Flags().GetString("last-name")
			em, _ := cmd.Flags().GetString("email")
			_ = newStore().UpdateUser(u, engine.UserUpdate{FirstName: &fn, LastName: &ln, Email: &em})
			fmt.Printf("user %s created with role %s\n", u, role)
		}}
	create.Flags().String("username", "", "username")
	create.Flags().String("role", "Viewer", "Viewer | User | Op | Admin")
	create.Flags().Bool("exists-ok", false, "do nothing if the user already exists (for container init)")
	_ = create.MarkFlagRequired("username")

	update := &cobra.Command{Use: "update <username>", Short: "Change a user's role, status, details or password", Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, a []string) {
			var up engine.UserUpdate
			f := cmd.Flags()
			str := func(name string) *string {
				if !f.Changed(name) {
					return nil
				}
				v, _ := f.GetString(name)
				return &v
			}
			up.FirstName, up.LastName, up.Email = str("first-name"), str("last-name"), str("email")
			if r := str("role"); r != nil {
				role, err := engine.ParseRole(*r)
				if err != nil {
					exitf("%v", err)
				}
				up.Role = &role
			}
			if f.Changed("active") {
				v, _ := f.GetBool("active")
				up.Active = &v
			}
			if v, _ := f.GetBool("reset-password"); v {
				pw := readPassword()
				up.Password = &pw
			}
			if err := newStore().UpdateUser(a[0], up); err != nil {
				exitf("%v", err)
			}
			fmt.Println("user updated:", a[0])
		}}
	update.Flags().String("role", "", "Viewer | User | Op | Admin")
	update.Flags().Bool("active", true, "--active=false deactivates the user")
	update.Flags().String("first-name", "", "first name")
	update.Flags().String("last-name", "", "last name")
	update.Flags().String("email", "", "email")
	update.Flags().Bool("reset-password", false, "prompt for a new password (or DAGSFLOW_PASSWORD); revokes API tokens")
	create.Flags().String("first-name", "", "first name")
	create.Flags().String("last-name", "", "last name")
	create.Flags().String("email", "", "email")

	c.AddCommand(create, update,
		&cobra.Command{Use: "list", Short: "List users", Run: func(*cobra.Command, []string) {
			users, err := newStore().Users()
			if err != nil {
				exitf("%v", err)
			}
			w := table()
			fmt.Fprintln(w, "USERNAME\tNAME\tEMAIL\tROLE\tACTIVE\tAPI_TOKENS\tLAST_LOGIN")
			for _, u := range users {
				last := "-"
				if u.LastLogin != nil {
					last = u.LastLogin.Local().Format(time.DateTime)
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%v\t%d\t%s\n", u.Username, strings.TrimSpace(u.FirstName+" "+u.LastName),
					u.Email, u.Role, u.Active(), len(u.Tokens), last)
			}
			w.Flush()
		}},
		&cobra.Command{Use: "delete <username>", Short: "Delete a user", Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, a []string) {
				if err := newStore().DeleteUser(a[0]); err != nil {
					exitf("%v", err)
				}
				fmt.Println("user deleted:", a[0])
			}},
		&cobra.Command{Use: "token <username>", Short: "Issue an API token (shown once)", Args: cobra.ExactArgs(1),
			Run: func(_ *cobra.Command, a []string) {
				tok, err := newStore().IssueToken(a[0])
				if err != nil {
					exitf("%v", err)
				}
				fmt.Println(tok)
				fmt.Fprintln(os.Stderr, "use as: Authorization: Bearer <token>")
			}},
	)
	return c
}

// ---- datasets ------------------------------------------------------------------------

func datasetsCmd() *cobra.Command {
	c := &cobra.Command{Use: "datasets", Short: "Inspect datasets (data-aware scheduling)"}
	c.AddCommand(&cobra.Command{Use: "list", Short: "List datasets, producers and consumers", Run: func(*cobra.Command, []string) {
		ds, err := newStore().Datasets()
		if err != nil {
			exitf("%v", err)
		}
		producers, consumers := datasetGraph()
		uris := map[string]bool{}
		for u := range ds {
			uris[u] = true
		}
		for u := range producers {
			uris[u] = true
		}
		for u := range consumers {
			uris[u] = true
		}
		list := make([]string, 0, len(uris))
		for u := range uris {
			list = append(list, u)
		}
		sort.Strings(list)
		w := table()
		fmt.Fprintln(w, "URI\tLAST_UPDATED\tPRODUCED_BY\tCONSUMED_BY")
		for _, u := range list {
			last := "-"
			if d := ds[u]; d != nil {
				last = d.LastUpdated.Local().Format(time.DateTime)
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", u, last, strings.Join(producers[u], ","), strings.Join(consumers[u], ","))
		}
		w.Flush()
	}})
	return c
}

func datasetGraph() (producers, consumers map[string][]string) {
	producers, consumers = map[string][]string{}, map[string][]string{}
	for _, d := range dag.List() {
		for _, u := range d.Datasets {
			consumers[u] = append(consumers[u], d.ID)
		}
		for _, t := range d.Tasks() {
			for _, u := range t.Outlets() {
				producers[u] = append(producers[u], d.ID+"."+t.ID)
			}
		}
	}
	return
}

// ---- config ----------------------------------------------------------------------------

func configCmd() *cobra.Command {
	c := &cobra.Command{Use: "config", Short: "Configuration helpers"}
	c.AddCommand(&cobra.Command{Use: "gen-key", Short: "Generate a DAGSFLOW_FERNET_KEY for encrypting variables/connections",
		Run: func(*cobra.Command, []string) { fmt.Println(engine.GenerateKey()) }})
	return c
}
