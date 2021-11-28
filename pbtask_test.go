package scheduler

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPbTaskToTask(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name         string
		queuePath    string
		taskIDPrefix string
		pbTask       *taskspb.Task
		want         *Task
		isError      bool
	}{
		{
			name:         "basic",
			queuePath:    "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
			taskIDPrefix: "test_",
			pbTask: &taskspb.Task{
				Name: "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/tasks/test_id_3b9aca02v1",
				ScheduleTime: &timestamppb.Timestamp{
					Seconds: 1,
					Nanos:   2,
				},
				MessageType: &taskspb.Task_HttpRequest{
					HttpRequest: &taskspb.HttpRequest{
						Url:        "https://example.com/",
						HttpMethod: taskspb.HttpMethod_POST,
						Headers: map[string]string{
							"Content-Type": "application/json",
						},
						Body: []byte(`{"payload":"test"}`),
						AuthorizationHeader: &taskspb.HttpRequest_OauthToken{
							OauthToken: &taskspb.OAuthToken{
								ServiceAccountEmail: "test@example.com",
								Scope:               "https://www.googleapis.com/auth/calendar",
							},
						},
					},
				},
			},
			want: &Task{
				QueuePath:   "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
				Prefix:      "test_",
				ID:          "id",
				ScheduledAt: time.Unix(1, 2).UTC(),
				Request: func() *http.Request {
					req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.com/", bytes.NewReader([]byte(`{"payload":"test"}`)))
					req.Header.Set("Content-Type", "application/json")
					return req
				}(),
				Authorization: &OAuthToken{
					ServiceAccountEmail: "test@example.com",
					Scope:               "https://www.googleapis.com/auth/calendar",
				},
				Version: 1,
			},
			isError: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := PbTaskToTask(ctx, tt.queuePath, tt.taskIDPrefix, tt.pbTask)
			if tt.isError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// ignore GetBody function
			tt.want.Request.GetBody = nil
			got.Request.GetBody = nil

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTaskToPbTask(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		task    *Task
		want    *taskspb.Task
		wantErr error
	}{
		{
			name: "unauthorized get request",
			task: &Task{
				QueuePath:   "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
				Prefix:      "test_",
				ID:          "get",
				ScheduledAt: time.Unix(10, 2).UTC(),
				Request: func() *http.Request {
					req, _ := http.NewRequest(http.MethodGet, "https://example.com/", nil)
					return req
				}(),
				Version: 1,
			},
			want: &taskspb.Task{
				Name: "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/tasks/test_get_2540be402v1",
				ScheduleTime: &timestamppb.Timestamp{
					Seconds: 10,
					Nanos:   2,
				},
				MessageType: &taskspb.Task_HttpRequest{
					HttpRequest: &taskspb.HttpRequest{
						Url:        "https://example.com/",
						HttpMethod: taskspb.HttpMethod_GET,
						Headers:    map[string]string{},
					},
				},
			},
		},
		{
			name: "post request with OAuthToken",
			task: &Task{
				QueuePath:   "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
				Prefix:      "test_",
				ID:          "id",
				ScheduledAt: time.Unix(1, 2).UTC(),
				Request: func() *http.Request {
					req, _ := http.NewRequest(http.MethodPost, "https://example.com/", bytes.NewReader([]byte(`{"payload":"test"}`)))
					req.Header.Set("Content-Type", "application/json")
					return req
				}(),
				Authorization: &OAuthToken{
					ServiceAccountEmail: "test@example.com",
					Scope:               "https://www.googleapis.com/auth/calendar",
				},
				Version: 1,
			},
			want: &taskspb.Task{
				Name: "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/tasks/test_id_3b9aca02v1",
				ScheduleTime: &timestamppb.Timestamp{
					Seconds: 1,
					Nanos:   2,
				},
				MessageType: &taskspb.Task_HttpRequest{
					HttpRequest: &taskspb.HttpRequest{
						Url:        "https://example.com/",
						HttpMethod: taskspb.HttpMethod_POST,
						Headers: map[string]string{
							"Content-Type": "application/json",
						},
						Body: []byte(`{"payload":"test"}`),
						AuthorizationHeader: &taskspb.HttpRequest_OauthToken{
							OauthToken: &taskspb.OAuthToken{
								ServiceAccountEmail: "test@example.com",
								Scope:               "https://www.googleapis.com/auth/calendar",
							},
						},
					},
				},
			},
		},
		{
			name: "post request with OIDCToken",
			task: &Task{
				QueuePath:   "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
				Prefix:      "test_",
				ID:          "id",
				ScheduledAt: time.Unix(1, 3).UTC(),
				Request: func() *http.Request {
					req, _ := http.NewRequest(http.MethodPost, "https://example.com/", bytes.NewReader([]byte(`{"payload":"test"}`)))
					req.Header.Set("Content-Type", "application/json")
					return req
				}(),
				Authorization: &OIDCToken{
					ServiceAccountEmail: "test@example.com",
					Audience:            "test",
				},
				Version: 1,
			},
			want: &taskspb.Task{
				Name: "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/tasks/test_id_3b9aca03v1",
				ScheduleTime: &timestamppb.Timestamp{
					Seconds: 1,
					Nanos:   3,
				},
				MessageType: &taskspb.Task_HttpRequest{
					HttpRequest: &taskspb.HttpRequest{
						Url:        "https://example.com/",
						HttpMethod: taskspb.HttpMethod_POST,
						Headers: map[string]string{
							"Content-Type": "application/json",
						},
						Body: []byte(`{"payload":"test"}`),
						AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
							OidcToken: &taskspb.OidcToken{
								ServiceAccountEmail: "test@example.com",
								Audience:            "test",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := TaskToPbTask(tt.task)
			assert.ErrorIs(t, err, tt.wantErr)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_convertTasksMutually(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name string
		task *Task
	}{
		{
			name: "post request with authorization",
			task: &Task{
				QueuePath:   "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
				Prefix:      "test_",
				ID:          "post",
				ScheduledAt: time.Unix(1, 2).UTC(),
				Request: func() *http.Request {
					req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.com/", bytes.NewReader([]byte(`{"payload":"test"}`)))
					req.Header.Set("Content-Type", "application/json")
					return req
				}(),
				Authorization: &OAuthToken{
					ServiceAccountEmail: "test@example.com",
					Scope:               "https://www.googleapis.com/auth/calendar",
				},
				Version: 1,
			},
		},
		{
			name: "get request without authorization",
			task: &Task{
				QueuePath:   "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
				Prefix:      "test_",
				ID:          "get",
				ScheduledAt: time.Unix(10, 1).UTC(),
				Request: func() *http.Request {
					req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.com/", nil)
					return req
				}(),
				Version: 2,
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pb, err := TaskToPbTask(tt.task)
			require.NoError(t, err)
			got, err := PbTaskToTask(ctx, tt.task.QueuePath, tt.task.Prefix, pb)
			require.NoError(t, err)

			// ignore GetBody function
			tt.task.Request.GetBody = nil
			got.Request.GetBody = nil

			assert.Equal(t, tt.task, got)
		})
	}
}
