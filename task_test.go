package scheduler

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestTask_TaskID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		task *Task
		want string
	}{
		{
			name: "generate taskID from prefix, id and scheduledAt",
			task: &Task{
				Prefix:      "pre-",
				ID:          "id",
				ScheduledAt: time.Unix(1, 234567890),
				Version:     1,
			},
			want: "pre-id_499602d2v1",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.task.TaskID()
			if got != tt.want {
				t.Errorf("got: %v, want: %v", got, tt.want)
			}
		})
	}
}

func TestTask_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		task *Task
		want error
	}{
		{
			name: "empty task id",
			task: &Task{
				Prefix:      "pre-",
				ID:          "",
				ScheduledAt: time.Unix(1, 234567890),
			},
			want: ErrTaskValidation,
		},
		{
			name: "task id contains invalid character (/)",
			task: &Task{
				Prefix:      "pre-",
				ID:          "a/b",
				ScheduledAt: time.Unix(1, 234567890),
			},
			want: ErrTaskValidation,
		},
		{
			name: "valid task id",
			task: &Task{
				Prefix:      "pre-",
				ID:          "a-b_c0",
				ScheduledAt: time.Unix(1, 234567890),
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.task.Validate()
			if !errors.Is(err, tt.want) {
				t.Errorf("got: %v, want: %v", err, tt.want)
			}
		})
	}
}

func TestTask_validateTaskID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		task *Task
		want error
	}{
		{
			name: "task id length over 500",
			task: &Task{
				Prefix:      "pre-",                                 // 4 caracters
				ID:          strings.Join(make([]string, 487), "_"), // 486 caracters
				ScheduledAt: time.Unix(1, 234567890),                // 9 caracters as encoded string
				Version:     1,                                      // 2 caracters
			},
			want: ErrTaskValidation,
		},
		{
			name: "task id length less then 500",
			task: &Task{
				Prefix:      "pre-",                                 // 4 caracters
				ID:          strings.Join(make([]string, 486), "_"), // 485 caracters
				ScheduledAt: time.Unix(1, 234567890),                // 9 caracters as encoded string
				Version:     1,                                      // 2 caracters
			},
			want: nil,
		},
		{
			name: "task id contains invalid character (/)",
			task: &Task{
				Prefix:      "pre-",
				ID:          "a/b",
				ScheduledAt: time.Unix(1, 234567890),
			},
			want: ErrTaskValidation,
		},
		{
			name: "task id contains invalid character (#)",
			task: &Task{
				Prefix:      "pre-",
				ID:          "#ab",
				ScheduledAt: time.Unix(1, 234567890),
			},
			want: ErrTaskValidation,
		},
		{
			name: "valid task id",
			task: &Task{
				Prefix:      "pre-",
				ID:          "a-b_c0",
				ScheduledAt: time.Unix(1, 234567890),
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.task.validateTaskID()
			if !errors.Is(err, tt.want) {
				t.Errorf("got: %v, want: %v", err, tt.want)
			}
		})
	}
}

func TestTask_Compare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		task   *Task
		target *Task
		want   bool
	}{
		{
			name: "same task",
			task: &Task{
				Prefix:      "pre-",
				ID:          "test",
				ScheduledAt: time.Unix(10, 1),
				Request: func() *http.Request {
					req, _ := http.NewRequest(http.MethodPost, "https://example.com", strings.NewReader("test"))
					req.Header.Set("User-Agent", "test-client")
					return req
				}(),
				Authorization: &OAuthToken{
					ServiceAccountEmail: "test@example.com",
					Scope:               "https://example.com/test",
				},
			},
			target: &Task{
				Prefix:      "pre-",
				ID:          "test",
				ScheduledAt: time.Unix(10, 1),
				Request: func() *http.Request {
					req, _ := http.NewRequest(http.MethodPost, "https://example.com", strings.NewReader("test"))
					req.Header.Set("User-Agent", "test-client")
					return req
				}(),
				Authorization: &OAuthToken{
					ServiceAccountEmail: "test@example.com",
					Scope:               "https://example.com/test",
				},
			},
			want: true,
		},
		{
			name: "task with different comparisonID",
			task: &Task{
				Prefix:      "pre-",
				ID:          "test",
				ScheduledAt: time.Unix(10, 1),
			},
			target: &Task{
				Prefix:      "pre-",
				ID:          "test",
				ScheduledAt: time.Unix(10, 2),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.task.Compare(tt.target)
			if got != tt.want {
				t.Errorf("got: %v, want: %v", got, tt.want)
			}
		})
	}
}

func TestParseTaskName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		prefix      string
		taskName    string
		wantID      string
		wantVersion int
		isError     bool
	}{
		{
			name:        "valid task name",
			prefix:      "pre-",
			taskName:    "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/pre-id_499602d2v1",
			wantID:      "id",
			wantVersion: 1,
		},
		{
			name:        "task name with trimmed queue path prefix",
			prefix:      "pre-",
			taskName:    "pre-id_499602d2v1",
			wantID:      "id",
			wantVersion: 1,
		},
		{
			name:        "invalid prefix",
			prefix:      "pre-",
			taskName:    "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/pre_id_499602d2v1",
			wantID:      "id",
			wantVersion: 1,
			isError:     true,
		},
		{
			name:        "invalid task timestamp separator",
			prefix:      "pre-",
			taskName:    "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/pre-id-499602d2v1",
			wantID:      "id",
			wantVersion: 1,
			isError:     true,
		},
		{
			name:        "invalid task version separator",
			prefix:      "pre-",
			taskName:    "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/pre-id_499602d2_1",
			wantID:      "id",
			wantVersion: 1,
			isError:     true,
		},
		{
			name:        "failed to parse version",
			prefix:      "pre-",
			taskName:    "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/pre-id_499602d2vf",
			wantID:      "id",
			wantVersion: 1,
			isError:     true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotID, gotVersion, err := ParseTaskName(tt.prefix, tt.taskName)
			if tt.isError {
				if err == nil {
					t.Fatal("no error occurred")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error occurred: %v", err)
			}
			if gotID != tt.wantID {
				t.Errorf("got: %v, want: %v", gotID, tt.wantID)
			}
			if gotVersion != tt.wantVersion {
				t.Errorf("got: %v, want: %v", gotVersion, tt.wantVersion)
			}
		})
	}
}
