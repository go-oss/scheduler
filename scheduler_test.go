package scheduler_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/go-oss/scheduler"
	mock_scheduler "github.com/go-oss/scheduler/mock"
)

func TestScheduler_Sync(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	remoteTasks := []*taskspb.Task{
		{
			Name:         "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/tasks/test_id_3b9aca02v1",
			ScheduleTime: &timestamppb.Timestamp{Seconds: 1, Nanos: 2},
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
		{
			Name:         "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/tasks/test_get_2540be401v2",
			ScheduleTime: &timestamppb.Timestamp{Seconds: 10, Nanos: 1},
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url:        "https://example.com/",
					HttpMethod: taskspb.HttpMethod_GET,
					Headers:    map[string]string{},
				},
			},
		},
	}
	localTasks := []*scheduler.Task{
		{
			QueuePath:   "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
			Prefix:      "test_",
			ID:          "id",
			ScheduledAt: time.Unix(1, 2).UTC(),
			Request: func() *http.Request {
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.com/", bytes.NewReader([]byte(`{"payload":"test"}`)))
				req.Header.Set("Content-Type", "application/json")
				return req
			}(),
			Authorization: &scheduler.OAuthToken{
				ServiceAccountEmail: "test@example.com",
				Scope:               "https://www.googleapis.com/auth/calendar",
			},
			Version: 1,
		},
		{
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
	}

	tests := []struct {
		name     string
		tasks    []*scheduler.Task
		injector func(*mock_scheduler.MockCloudTasksClient, *mock_scheduler.MockTaskLister, *mock_scheduler.MockTaskIterator)
		want     error
	}{
		{
			name:  "no tasks",
			tasks: nil,
			injector: func(m *mock_scheduler.MockCloudTasksClient, l *mock_scheduler.MockTaskLister, i *mock_scheduler.MockTaskIterator) {
				l.EXPECT().ListTasks(ctx, &taskspb.ListTasksRequest{
					Parent:       "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
					ResponseView: taskspb.Task_BASIC,
					PageSize:     1000,
					PageToken:    "",
				}).Return(i)
				i.EXPECT().PageInfo().Return(&iterator.PageInfo{})
				i.EXPECT().Next().Return(nil, scheduler.Done)
			},
			want: nil,
		},
		{
			name:  "no update tasks",
			tasks: localTasks,
			injector: func(m *mock_scheduler.MockCloudTasksClient, l *mock_scheduler.MockTaskLister, i *mock_scheduler.MockTaskIterator) {
				l.EXPECT().ListTasks(ctx, &taskspb.ListTasksRequest{
					Parent:       "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
					ResponseView: taskspb.Task_BASIC,
					PageSize:     1000,
					PageToken:    "",
				}).Return(i)
				i.EXPECT().PageInfo().Return(&iterator.PageInfo{})
				i.EXPECT().Next().Return(remoteTasks[0], nil)
				i.EXPECT().Next().Return(remoteTasks[1], nil)
				i.EXPECT().Next().Return(nil, scheduler.Done)
			},
			want: nil,
		},
		{
			name:  "create tasks",
			tasks: localTasks,
			injector: func(m *mock_scheduler.MockCloudTasksClient, l *mock_scheduler.MockTaskLister, i *mock_scheduler.MockTaskIterator) {
				l.EXPECT().ListTasks(ctx, &taskspb.ListTasksRequest{
					Parent:       "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
					ResponseView: taskspb.Task_BASIC,
					PageSize:     1000,
					PageToken:    "",
				}).Return(i)
				i.EXPECT().PageInfo().Return(&iterator.PageInfo{})
				i.EXPECT().Next().Return(nil, scheduler.Done)
				m.EXPECT().CreateTask(ctx, &taskspb.CreateTaskRequest{
					Parent:       "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
					Task:         remoteTasks[0],
					ResponseView: taskspb.Task_BASIC,
				}).Return(remoteTasks[0], nil)
				m.EXPECT().CreateTask(ctx, &taskspb.CreateTaskRequest{
					Parent:       "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
					Task:         remoteTasks[1],
					ResponseView: taskspb.Task_BASIC,
				}).Return(remoteTasks[1], nil)
			},
			want: nil,
		},
		{
			name: "update tasks",
			tasks: []*scheduler.Task{
				{
					QueuePath:   "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
					Prefix:      "test_",
					ID:          "id",
					ScheduledAt: time.Unix(1, 2).UTC(),
					Request: func() *http.Request {
						req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.com/", bytes.NewReader([]byte(`{"payload":"test"}`)))
						req.Header.Set("Content-Type", "application/json")
						return req
					}(),
					Authorization: &scheduler.OAuthToken{
						ServiceAccountEmail: "test@example.com",
						Scope:               "https://www.googleapis.com/auth/calendar",
					},
					Version: 3,
				},
				{
					QueuePath:   "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
					Prefix:      "test_",
					ID:          "get",
					ScheduledAt: time.Unix(10, 1).UTC(),
					Request: func() *http.Request {
						req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.com/test", nil)
						return req
					}(),
					Version: 2,
				},
			},
			injector: func(m *mock_scheduler.MockCloudTasksClient, l *mock_scheduler.MockTaskLister, i *mock_scheduler.MockTaskIterator) {
				l.EXPECT().ListTasks(ctx, &taskspb.ListTasksRequest{
					Parent:       "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
					ResponseView: taskspb.Task_BASIC,
					PageSize:     1000,
					PageToken:    "",
				}).Return(i)
				i.EXPECT().PageInfo().Return(&iterator.PageInfo{})
				i.EXPECT().Next().Return(remoteTasks[0], nil)
				i.EXPECT().Next().Return(remoteTasks[1], nil)
				i.EXPECT().Next().Return(nil, scheduler.Done)
				tasks := []*taskspb.Task{
					{
						Name:         "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/tasks/test_id_3b9aca02v3",
						ScheduleTime: &timestamppb.Timestamp{Seconds: 1, Nanos: 2},
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
					{
						Name:         "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/tasks/test_get_2540be401v3",
						ScheduleTime: &timestamppb.Timestamp{Seconds: 10, Nanos: 1},
						MessageType: &taskspb.Task_HttpRequest{
							HttpRequest: &taskspb.HttpRequest{
								Url:        "https://example.com/test",
								HttpMethod: taskspb.HttpMethod_GET,
								Headers:    map[string]string{},
							},
						},
					},
				}
				m.EXPECT().DeleteTask(ctx, &taskspb.DeleteTaskRequest{
					Name: remoteTasks[0].Name,
				}).Return(nil)
				m.EXPECT().DeleteTask(ctx, &taskspb.DeleteTaskRequest{
					Name: remoteTasks[1].Name,
				}).Return(nil)
				m.EXPECT().CreateTask(ctx, &taskspb.CreateTaskRequest{
					Parent:       "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
					Task:         tasks[0],
					ResponseView: taskspb.Task_BASIC,
				}).Return(tasks[0], nil)
				m.EXPECT().CreateTask(ctx, &taskspb.CreateTaskRequest{
					Parent:       "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
					Task:         tasks[1],
					ResponseView: taskspb.Task_BASIC,
				}).Return(tasks[1], nil)
			},
			want: nil,
		},
		{
			name:  "delete tasks",
			tasks: nil,
			injector: func(m *mock_scheduler.MockCloudTasksClient, l *mock_scheduler.MockTaskLister, i *mock_scheduler.MockTaskIterator) {
				l.EXPECT().ListTasks(ctx, &taskspb.ListTasksRequest{
					Parent:       "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
					ResponseView: taskspb.Task_BASIC,
					PageSize:     1000,
					PageToken:    "",
				}).Return(i)
				i.EXPECT().PageInfo().Return(&iterator.PageInfo{})
				i.EXPECT().Next().Return(remoteTasks[0], nil)
				i.EXPECT().Next().Return(remoteTasks[1], nil)
				i.EXPECT().Next().Return(nil, scheduler.Done)
				m.EXPECT().DeleteTask(ctx, &taskspb.DeleteTaskRequest{
					Name: remoteTasks[0].Name,
				}).Return(nil)
				m.EXPECT().DeleteTask(ctx, &taskspb.DeleteTaskRequest{
					Name: remoteTasks[1].Name,
				}).Return(nil)
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			m := mock_scheduler.NewMockCloudTasksClient(ctrl)
			l := mock_scheduler.NewMockTaskLister(ctrl)
			i := mock_scheduler.NewMockTaskIterator(ctrl)
			tt.injector(m, l, i)

			s := scheduler.New(m, "tokyo-rain-123", "asia-northeast1", "scheduler", "test_")
			s.SetIterator(func(opts ...gax.CallOption) *scheduler.Iterator {
				return scheduler.NewIterator(l, "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler", "test_", opts...)
			})

			err := s.Sync(ctx, tt.tasks)
			if !errors.Is(err, tt.want) {
				t.Errorf("got: %v, want: %v", err, tt.want)
			}
		})
	}
}

func TestScheduler_List(t *testing.T) {
	t.Parallel()

	mockCli := mock_scheduler.NewMockCloudTasksClient(gomock.NewController(t))
	tests := []struct {
		name     string
		project  string
		location string
		queue    string
		prefix   string
		want     *scheduler.Iterator
	}{
		{
			name:     "list returns iterator",
			project:  "tokyo-rain-123",
			location: "asia-northeast1",
			queue:    "scheduler",
			prefix:   "list-",
			want: scheduler.NewIterator(
				nil,
				"projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler",
				"list-",
			),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := scheduler.New(mockCli, tt.project, tt.location, tt.queue, tt.prefix)
			it := s.List()
			it.SetLister(nil) // func is not comparable
			assert.Equal(t, tt.want, it)
		})
	}
}

func TestScheduler_Create(t *testing.T) {
	t.Parallel()

	errUnexpected := errors.New("unexpected error")
	ctx := context.Background()
	tests := []struct {
		name     string
		project  string
		location string
		queue    string
		prefix   string
		task     *scheduler.Task
		injector func(*mock_scheduler.MockCloudTasksClient, *scheduler.Task)
		wantErr  error
	}{
		{
			name:     "task validation failed",
			project:  "tokyo-rain-123",
			location: "asia-northeast1",
			queue:    "test",
			prefix:   "pre-",
			task:     &scheduler.Task{},
			injector: func(m *mock_scheduler.MockCloudTasksClient, task *scheduler.Task) {},
			wantErr:  scheduler.ErrTaskValidation,
		},
		{
			name:     "already exists",
			project:  "tokyo-rain-123",
			location: "asia-northeast1",
			queue:    "test",
			prefix:   "pre-",
			task: &scheduler.Task{
				QueuePath:   "projects/tokyo-rain-123/locations/asia-northeast1/queues/test",
				Prefix:      "pre-",
				ID:          "task",
				ScheduledAt: time.Unix(10, 1),
				Request: func() *http.Request {
					req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
					return req
				}(),
				Authorization: &scheduler.OAuthToken{
					ServiceAccountEmail: "test@example.com",
					Scope:               "https://example.com/test",
				},
				Version: 1,
			},
			injector: func(m *mock_scheduler.MockCloudTasksClient, task *scheduler.Task) {
				pbtask, err := scheduler.TaskToPbTask(task)
				if err != nil {
					panic(err)
				}

				m.EXPECT().CreateTask(ctx, &taskspb.CreateTaskRequest{
					Parent:       task.QueuePath,
					Task:         pbtask,
					ResponseView: taskspb.Task_BASIC,
				}).Return(nil, status.Error(codes.AlreadyExists, "already exists"))
			},
			wantErr: scheduler.ErrTaskAlreadyExists,
		},
		{
			name:     "unexpected error",
			project:  "tokyo-rain-123",
			location: "asia-northeast1",
			queue:    "test",
			prefix:   "pre-",
			task: &scheduler.Task{
				QueuePath:   "projects/tokyo-rain-123/locations/asia-northeast1/queues/test",
				Prefix:      "pre-",
				ID:          "task",
				ScheduledAt: time.Unix(10, 1),
				Request: func() *http.Request {
					req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
					return req
				}(),
				Authorization: &scheduler.OAuthToken{
					ServiceAccountEmail: "test@example.com",
					Scope:               "https://example.com/test",
				},
				Version: 1,
			},
			injector: func(m *mock_scheduler.MockCloudTasksClient, task *scheduler.Task) {
				pbtask, err := scheduler.TaskToPbTask(task)
				if err != nil {
					panic(err)
				}

				m.EXPECT().CreateTask(ctx, &taskspb.CreateTaskRequest{
					Parent:       task.QueuePath,
					Task:         pbtask,
					ResponseView: taskspb.Task_BASIC,
				}).Return(nil, errUnexpected)
			},
			wantErr: errUnexpected,
		},
		{
			name:     "success",
			project:  "tokyo-rain-123",
			location: "asia-northeast1",
			queue:    "test",
			prefix:   "pre-",
			task: &scheduler.Task{
				QueuePath:   "projects/tokyo-rain-123/locations/asia-northeast1/queues/test",
				Prefix:      "pre-",
				ID:          "task",
				ScheduledAt: time.Unix(10, 1),
				Request: func() *http.Request {
					req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
					return req
				}(),
				Authorization: &scheduler.OAuthToken{
					ServiceAccountEmail: "test@example.com",
					Scope:               "https://example.com/test",
				},
				Version: 1,
			},
			injector: func(m *mock_scheduler.MockCloudTasksClient, task *scheduler.Task) {
				pbtask, err := scheduler.TaskToPbTask(task)
				if err != nil {
					panic(err)
				}

				m.EXPECT().CreateTask(ctx, &taskspb.CreateTaskRequest{
					Parent:       task.QueuePath,
					Task:         pbtask,
					ResponseView: taskspb.Task_BASIC,
				}).Return(pbtask, nil)
			},
		},
		{
			name:     "set task version",
			project:  "tokyo-rain-123",
			location: "asia-northeast1",
			queue:    "test",
			prefix:   "pre-",
			task: &scheduler.Task{
				QueuePath:   "projects/tokyo-rain-123/locations/asia-northeast1/queues/test",
				Prefix:      "pre-",
				ID:          "task",
				ScheduledAt: time.Unix(10, 1),
				Request: func() *http.Request {
					req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
					return req
				}(),
				Authorization: &scheduler.OAuthToken{
					ServiceAccountEmail: "test@example.com",
					Scope:               "https://example.com/test",
				},
				Version: 0,
			},
			injector: func(m *mock_scheduler.MockCloudTasksClient, task *scheduler.Task) {
				task.Version = 1
				pbtask, err := scheduler.TaskToPbTask(task)
				if err != nil {
					panic(err)
				}

				m.EXPECT().CreateTask(ctx, &taskspb.CreateTaskRequest{
					Parent:       task.QueuePath,
					Task:         pbtask,
					ResponseView: taskspb.Task_BASIC,
				}).Return(pbtask, nil)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			m := mock_scheduler.NewMockCloudTasksClient(ctrl)
			tt.injector(m, tt.task)

			s := scheduler.New(m, tt.project, tt.location, tt.queue, tt.prefix)
			err := s.Create(ctx, tt.task)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("got: %v, want: %v", err, tt.wantErr)
			}
		})
	}
}

func TestScheduler_Delete(t *testing.T) {
	t.Parallel()

	errUnexpected := errors.New("unexpected error")
	ctx := context.Background()
	tests := []struct {
		name     string
		taskName string
		injector func(*mock_scheduler.MockCloudTasksClient)
		wantErr  error
	}{
		{
			name:     "unexpected error",
			taskName: "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/test_delete_499602d2v1",
			injector: func(m *mock_scheduler.MockCloudTasksClient) {
				m.EXPECT().DeleteTask(ctx, &taskspb.DeleteTaskRequest{
					Name: "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/test_delete_499602d2v1",
				}).Return(errUnexpected)
			},
			wantErr: errUnexpected,
		},
		{
			name:     "delete task",
			taskName: "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/test_delete_499602d2v1",
			injector: func(m *mock_scheduler.MockCloudTasksClient) {
				m.EXPECT().DeleteTask(ctx, &taskspb.DeleteTaskRequest{
					Name: "projects/tokyo-rain-123/locations/asia-northeast1/queues/scheduler/test_delete_499602d2v1",
				}).Return(nil)
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			m := mock_scheduler.NewMockCloudTasksClient(ctrl)
			tt.injector(m)

			s := scheduler.New(m, "tokyo-rain-123", "asia-northeast1", "scheduler", "test_")
			err := s.Delete(ctx, tt.taskName)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("got: %v, want: %v", err, tt.wantErr)
			}
		})
	}
}
