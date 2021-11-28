//go:generate go run github.com/golang/mock/mockgen -destination=mock/$GOFILE -package=mock_$GOPACKAGE -source=$GOFILE

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"github.com/googleapis/gax-go"
	"google.golang.org/api/iterator"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
)

var Done = iterator.Done

type TaskLister interface {
	ListTasks(ctx context.Context, req *taskspb.ListTasksRequest, opts ...gax.CallOption) TaskIterator
}

type TaskListerFunc func(ctx context.Context, req *taskspb.ListTasksRequest, opts ...gax.CallOption) *cloudtasks.TaskIterator

func (f TaskListerFunc) ListTasks(ctx context.Context, req *taskspb.ListTasksRequest, opts ...gax.CallOption) TaskIterator {
	return f(ctx, req, opts...)
}

type TaskIterator interface {
	Next() (*taskspb.Task, error)
	PageInfo() *iterator.PageInfo
}

type Iterator struct {
	lister       TaskLister
	iter         TaskIterator
	opts         []gax.CallOption
	queuePath    string
	taskIDPrefix string
	pageToken    string
}

func NewIterator(t TaskLister, queuePath, prefix string, opts ...gax.CallOption) *Iterator {
	return &Iterator{
		lister:       t,
		opts:         opts,
		queuePath:    queuePath,
		taskIDPrefix: prefix,
	}
}

func (i *Iterator) Next(ctx context.Context) (*Task, error) {
	if i.iter == nil {
		i.listTasks(ctx)
	}

	for {
		task, err := i.iter.Next()
		if errors.Is(err, iterator.Done) {
			if i.pageToken == "" {
				return nil, Done
			}
			i.listTasks(ctx)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate: %w", err)
		}

		// ignore task which has unmatched prefix
		taskNamePrefix := taskName(i.queuePath, i.taskIDPrefix)
		if !strings.HasPrefix(task.Name, taskNamePrefix) {
			continue
		}

		t, err := PbTaskToTask(ctx, i.queuePath, i.taskIDPrefix, task)
		if err != nil {
			return nil, fmt.Errorf("failed to convert task from pbtask: %w", err)
		}

		return t, nil
	}
}

func (i *Iterator) listTasks(ctx context.Context) {
	req := &taskspb.ListTasksRequest{
		Parent:       i.queuePath,
		ResponseView: taskspb.Task_BASIC,
		PageSize:     1000,
		PageToken:    i.pageToken,
	}
	i.iter = i.lister.ListTasks(ctx, req, i.opts...)
	i.pageToken = i.iter.PageInfo().Token
}
