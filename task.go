package scheduler

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	taskTimestampSeparator = "_"
	taskVersionSeparator   = "v"
)

type Task struct {
	QueuePath     string
	Prefix        string
	ID            string
	ScheduledAt   time.Time
	Request       *http.Request
	Authorization isAuthorizationToken
	Version       int
}

func (t *Task) comparisonID() string {
	return t.Prefix + t.ID + taskTimestampSeparator + strconv.FormatInt(t.ScheduledAt.UnixNano(), 16)
}

func (t *Task) TaskID() string {
	return t.comparisonID() + taskVersionSeparator + strconv.Itoa(t.Version)
}

func (t *Task) TaskName() string {
	return taskName(t.QueuePath, t.TaskID())
}

func taskName(queuePath, taskID string) string {
	return queuePath + "/tasks/" + taskID
}

func (t *Task) Validate() error {
	if t.ID == "" {
		return fmt.Errorf("ID is empty: %w", ErrTaskValidation)
	}

	if err := t.validateTaskID(); err != nil {
		return err
	}

	return nil
}

// validateTaskID validates task id.
// TASK_ID can contain only letters ([A-Za-z]), numbers ([0-9]), hyphens (-), or underscores (_). The maximum length is 500 characters.
// see: https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks#resource:-task
func (t *Task) validateTaskID() error {
	taskID := t.TaskID()

	if len(taskID) > 500 {
		return fmt.Errorf("task id maximum length is 500: %w", ErrTaskValidation)
	}

	for _, char := range taskID {
		if ('A' <= char && char <= 'Z') || ('a' <= char && char <= 'z') ||
			('0' <= char && char <= '9') ||
			char == '-' || char == '_' {
			continue
		}

		return fmt.Errorf("task id contains invalid character %c: %w", char, ErrTaskValidation)
	}

	return nil
}

func (t *Task) Compare(target *Task) bool {
	if t.comparisonID() != target.comparisonID() {
		return false
	}

	// t.Request.Body and t.Request.Header are not checked but use Version field to update tasks.
	if t.Version > target.Version {
		return false
	}

	if !reflect.DeepEqual(t.Authorization, target.Authorization) {
		return false
	}

	if t.Request.Method != target.Request.Method ||
		removeTrailingSlash(t.Request.URL.String()) != removeTrailingSlash(target.Request.URL.String()) {
		return false
	}

	return true
}

type isAuthorizationToken interface {
	isAuthorizationToken()
}

type OAuthToken struct {
	ServiceAccountEmail string
	Scope               string
}

func (*OAuthToken) isAuthorizationToken() {}

type OIDCToken struct {
	ServiceAccountEmail string
	Audience            string
}

func (*OIDCToken) isAuthorizationToken() {}

func removeTrailingSlash(u string) string {
	return strings.TrimSuffix(u, "/")
}

func ParseTaskName(prefix, taskName string) (string, int, error) {
	v := path.Base(taskName)

	if !strings.HasPrefix(v, prefix) {
		return "", 0, errors.New("task name has no valid prefix")
	}

	v = strings.TrimPrefix(v, prefix)
	tsIdx := strings.LastIndex(v, taskTimestampSeparator)
	if tsIdx < 0 {
		return "", 0, errors.New("invalid task name format")
	}

	id := v[:tsIdx]
	v = v[tsIdx+1:]
	vIdx := strings.LastIndex(v, taskVersionSeparator)
	if vIdx < 0 {
		return "", 0, errors.New("task name has no valid version")
	}

	version, err := strconv.Atoi(v[vIdx+1:])
	if err != nil {
		return "", 0, fmt.Errorf("failed to parse version: %w", err)
	}

	return id, version, nil
}
