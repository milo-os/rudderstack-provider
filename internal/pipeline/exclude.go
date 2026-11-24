package pipeline

import (
	"strings"

	"go.miloapis.net/rudderstack-provider/internal/audit"
	"go.miloapis.net/rudderstack-provider/internal/rudderstack"
)

// Exclusions are accounts whose activity is never sent to RudderStack, such
// as internal test users, canaries and integration suites. They are set per
// environment, so the shared TrackingRules stay free of environment IDs.
//
// Excluding a user drops what that user does and the calls about them. A
// resource they created can still be removed by someone else, such as the
// garbage collector, and that removal is sent; downstream queries count a
// removal without an add as nothing.
type Exclusions struct {
	// Users match the acting user's uid or username, and a call's userId.
	Users map[string]bool
	// Projects match the project a request was made in, and a project being
	// created or deleted.
	Projects map[string]bool
	// Orgs match the organization a request was made in or a call is for,
	// and an organization being created or deleted.
	Orgs map[string]bool
}

// NewExclusions builds Exclusions from lists of IDs. Empty entries are
// ignored.
func NewExclusions(users, projects, orgs []string) Exclusions {
	return Exclusions{Users: set(users), Projects: set(projects), Orgs: set(orgs)}
}

func set(ids []string) map[string]bool {
	m := map[string]bool{}
	for _, id := range ids {
		if id = strings.TrimSpace(id); id != "" {
			m[id] = true
		}
	}
	return m
}

// Len is the number of excluded IDs.
func (x Exclusions) Len() int { return len(x.Users) + len(x.Projects) + len(x.Orgs) }

// event reports whether the request was made by or in an excluded account,
// or creates or deletes one.
func (x Exclusions) event(ev *audit.Event) bool {
	if x.Len() == 0 {
		return false
	}
	if x.Users[ev.User.UID] || x.Users[ev.User.Username] {
		return true
	}
	t := ev.Tenant()
	switch {
	case strings.EqualFold(t.Type, "project") && x.Projects[t.Name]:
		return true
	case strings.EqualFold(t.Type, "organization") && x.Orgs[t.Name]:
		return true
	}
	if r := ev.ObjectRef; r != nil && r.APIGroup == "resourcemanager.miloapis.com" {
		switch r.Resource {
		case "projects":
			return x.Projects[r.Name]
		case "organizations":
			return x.Orgs[r.Name]
		}
	}
	return false
}

// call reports whether a call is about an excluded user or organization.
// groupID is the call's resolved organization.
func (x Exclusions) call(m rudderstack.Message, groupID string) bool {
	return (m.UserID != "" && x.Users[m.UserID]) || (groupID != "" && x.Orgs[groupID])
}
