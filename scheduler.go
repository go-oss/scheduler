//go:generate go run github.com/golang/mock/mockgen -destination=mock/$GOFILE -package=mock_$GOPACKAGE -source=$GOFILE

package scheduler

import (
	"context"
	"errors"
	"fmt"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"github.com/googleapis/gax-go"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrTaskValidation    = errors.New("task validation error")
	ErrTaskAlreadyExists = errors.New("task already exists")
)

type CloudTasksClient interface {
	ListTasks(ctx context.Context, req *taskspb.ListTasksRequest, opts ...gax.CallOption) *cloudtasks.TaskIterator
	CreateTask(ctx context.Context, req *taskspb.CreateTaskRequest, opts ...gax.CallOption) (*taskspb.Task, error)
	DeleteTask(ctx context.Context, req *taskspb.DeleteTaskRequest, opts ...gax.CallOption) error
}

type Scheduler struct {
	client    CloudTasksClient
	queuePath string
	prefix    string
	iterator  func(...gax.CallOption) *Iterator
}

func QueuePath(projectID, location, queue string) string {
	return "projects/" + projectID + "/locations/" + location + "/queues/" + queue
}

func New(client CloudTasksClient, projectID, location, queue, prefix string) *Scheduler {
	queuePath := QueuePath(projectID, location, queue)
	return &Scheduler{
		client:    client,
		queuePath: queuePath,
		prefix:    prefix,
		iterator: func(opts ...gax.CallOption) *Iterator {
			return NewIterator(TaskListerFunc(client.ListTasks), queuePath, prefix, opts...)
		},
	}
}

func (s *Scheduler) Sync(ctx context.Context, tasks []*Task, opts ...gax.CallOption) error {
	taskMap := make(map[string]*Task, len(tasks))
	for _, t := range tasks {
		taskMap[t.comparisonID()] = t
	}

	iter := s.List(opts...)
	var deleteNames []string
	for {
		remoteTask, err := iter.Next(ctx)
		if err != nil {
			if errors.Is(err, Done) {
				break
			}
			return fmt.Errorf("failed to iterate remoteTasks: %w", err)
		}

		if t, ok := taskMap[remoteTask.comparisonID()]; ok {
			if t.Compare(remoteTask) {
				delete(taskMap, t.comparisonID())
				continue
			}

			// delete remote task to update it
			deleteNames = append(deleteNames, remoteTask.TaskName())
			if t.Version <= remoteTask.Version {
				t.Version = remoteTask.Version + 1
			}
			continue
		}

		// delete remote task
		deleteNames = append(deleteNames, remoteTask.TaskName())
	}

	for _, name := range deleteNames {
		if err := s.Delete(ctx, name, opts...); err != nil {
			return err
		}
	}

	for _, t := range tasks {
		if _, ok := taskMap[t.comparisonID()]; !ok {
			continue
		}

		if err := s.Create(ctx, t, opts...); err != nil {
			return err
		}
	}

	return nil
}

func (s *Scheduler) List(opts ...gax.CallOption) *Iterator {
	return s.iterator(opts...)
}

func (s *Scheduler) Create(ctx context.Context, task *Task, opts ...gax.CallOption) error {
	if task.Version == 0 {
		task.Version = 1
	}

	if err := task.Validate(); err != nil {
		return err
	}

	t, err := TaskToPbTask(task)
	if err != nil {
		return err
	}

	req := &taskspb.CreateTaskRequest{
		Parent:       task.QueuePath,
		Task:         t,
		ResponseView: taskspb.Task_BASIC,
	}
	if _, err := s.client.CreateTask(ctx, req, opts...); err != nil {
		switch status.Code(err) {
		case codes.AlreadyExists:
			return ErrTaskAlreadyExists
		default:
			return err
		}
	}

	return nil
}

func (s *Scheduler) Delete(ctx context.Context, taskName string, opts ...gax.CallOption) error {
	req := &taskspb.DeleteTaskRequest{
		Name: taskName,
	}
	if err := s.client.DeleteTask(ctx, req, opts...); err != nil {
		return err
	}

	return nil
}
