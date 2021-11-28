package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"github.com/go-oss/scheduler"
)

const (
	// FIXME: Set GCP project id.
	projectID = "set-your-gcp-project-id"

	location = "asia-northeast1"
	queue    = "test"
	prefix   = "tasks_"
)

// FIXME: Set future date.
var scheduledAt = time.Date(2021, 12, 1, 12, 30, 50, 0, time.Local)

func main() {
	ctx := context.Background()
	cli, err := cloudtasks.NewClient(ctx)
	if err != nil {
		panic(err)
	}

	sc := scheduler.New(cli, projectID, location, queue, prefix)

	req, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err != nil {
		panic(err)
	}
	task := &scheduler.Task{
		QueuePath:   scheduler.QueuePath(projectID, location, queue),
		Prefix:      prefix,
		ID:          "test",
		ScheduledAt: scheduledAt,
		Request:     req,
		Version:     1,
	}

	if err := sc.Sync(ctx, []*scheduler.Task{task}); err != nil {
		panic(err)
	}

	it := sc.List()
	for {
		task, err := it.Next(ctx)
		if errors.Is(err, scheduler.Done) {
			break
		}
		if err != nil {
			panic(err)
		}

		log.Printf("task: %#v", task)
	}
}
