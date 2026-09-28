package engine

import (
	"strings"
	"testing"

	"dagsflow-go/dag"
)

func TestRBACRoles(t *testing.T) {
	st := NewStore(t.TempDir())
	d := dag.NewDAG("finance_daily", "", dag.WithTags("finance"))
	d.NewTask("x", func(*dag.TaskContext) error { return nil })
	dag.Register(d)

	for _, bad := range []string{"dag.fly:*", "pools.read:x", "dag.read:tag:", "dag.read:[bad"} {
		if _, err := ParsePermission(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
	perms := func(ss ...string) []Permission {
		var out []Permission
		for _, s := range ss {
			p, err := ParsePermission(s)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, p)
		}
		return out
	}
	if err := st.CreateRole(RoleDef{Name: "sales", Permissions: perms("dag.read:sales_*", "dag.trigger:sales_daily", "dag.edit:tag:finance", "pools.read")}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRole(RoleDef{Name: "SALES"}); err == nil {
		t.Fatal("duplicate role (case-insensitive) accepted")
	}
	if err := st.CreateRole(RoleDef{Name: "Admin"}); err == nil {
		t.Fatal("built-in role name accepted")
	}
	if err := st.UpdateRole("Viewer", RoleDef{}); err == nil {
		t.Fatal("built-in role changed")
	}
	if err := st.CreateUser("ana", "password123", "Viewer", "nope"); err == nil {
		t.Fatal("unknown role accepted")
	}
	if err := st.CreateUser("ana", "password123", "sales"); err != nil {
		t.Fatal(err)
	}
	roles, _ := st.Roles()
	users, _ := st.Users()
	a := AccessFor(users["ana"], roles)
	cases := []struct {
		act, dag string
		want     bool
	}{
		{ActDagRead, "sales_daily", true},
		{ActDagRead, "hr_daily", false},
		{ActDagTrigger, "sales_daily", true},
		{ActDagTrigger, "sales_weekly", false},
		{ActDagEdit, "finance_daily", true}, // by tag
		{ActDagRead, "finance_daily", true}, // implied by edit
		{ActDagDelete, "sales_daily", false},
	}
	for _, c := range cases {
		if got := a.DAG(c.act, c.dag); got != c.want {
			t.Errorf("%s on %s = %v", c.act, c.dag, got)
		}
	}
	if !a.Can(ActPoolsRead) || a.Can(ActPoolsEdit) || a.Can(ActUsersManage) {
		t.Fatalf("global: %v", a.Global())
	}
	if err := st.DeleteRole("sales"); err == nil || !strings.Contains(err.Error(), "ana") {
		t.Fatalf("delete assigned role: %v", err)
	}
	// multiple roles: union
	if err := st.UpdateUser("ana", UserUpdate{Roles: &[]string{"sales", "Viewer"}}); err != nil {
		t.Fatal(err)
	}
	users, _ = st.Users()
	if a := AccessFor(users["ana"], roles); !a.DAG(ActDagRead, "hr_daily") || a.DAG(ActDagEdit, "hr_daily") {
		t.Fatal("union of roles")
	}
	// update role permissions
	if err := st.UpdateRole("sales", RoleDef{Permissions: perms("dag.read:*")}); err != nil {
		t.Fatal(err)
	}
	roles, _ = st.Roles()
	if a := AccessFor(users["ana"], roles); a.DAG(ActDagTrigger, "sales_daily") {
		t.Fatal("old permission kept after update")
	}
	// the last admin keeps admin
	_ = st.CreateUser("root", "password123", "Admin")
	if err := st.UpdateUser("root", UserUpdate{Roles: &[]string{"sales"}}); err == nil {
		t.Fatal("last admin demoted")
	}
	// legacy user record (single role field)
	legacy := &User{Username: "old", Role: RoleOp}
	if a := AccessFor(legacy, roles); !a.Can(ActVarsEdit) || !a.DAG(ActDagDelete, "anything") {
		t.Fatal("legacy role")
	}
	_ = st.UpdateUser("ana", UserUpdate{Roles: &[]string{"Viewer"}})
	if err := st.DeleteRole("sales"); err != nil {
		t.Fatal(err)
	}
}
