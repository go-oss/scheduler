package scheduler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func PbTaskToTask(ctx context.Context, queuePath, taskIDPrefix string, pb *taskspb.Task) (*Task, error) {
	id, version, err := ParseTaskName(taskIDPrefix, pb.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to parse task name: %w", err)
	}

	var req *http.Request
	var authorizationToken isAuthorizationToken
	switch mt := pb.MessageType.(type) {
	case *taskspb.Task_HttpRequest:
		r, err := convertHTTPRequest(ctx, mt.HttpRequest)
		if err != nil {
			return nil, err
		}
		req = r

		at, err := convertAuthorizationToken(mt.HttpRequest)
		if err != nil {
			return nil, err
		}
		authorizationToken = at
	default:
		return nil, fmt.Errorf("unsupported message type: %s", pb.MessageType)
	}

	return &Task{
		QueuePath:     queuePath,
		Prefix:        taskIDPrefix,
		ID:            id,
		ScheduledAt:   pb.ScheduleTime.AsTime(),
		Request:       req,
		Authorization: authorizationToken,
		Version:       version,
	}, nil
}

func convertHTTPRequest(ctx context.Context, req *taskspb.HttpRequest) (*http.Request, error) {
	var method string
	switch req.HttpMethod {
	case taskspb.HttpMethod_POST:
		method = http.MethodPost
	case taskspb.HttpMethod_GET:
		method = http.MethodGet
	case taskspb.HttpMethod_HEAD:
		method = http.MethodHead
	case taskspb.HttpMethod_PUT:
		method = http.MethodPut
	case taskspb.HttpMethod_DELETE:
		method = http.MethodDelete
	case taskspb.HttpMethod_PATCH:
		method = http.MethodPatch
	case taskspb.HttpMethod_OPTIONS:
		method = http.MethodOptions
	default:
		return nil, fmt.Errorf("unsupported http method: %s", req.HttpMethod.String())
	}

	var body io.Reader
	if req.Body != nil {
		body = bytes.NewReader(req.Body)
	}
	r, err := http.NewRequestWithContext(ctx, method, req.Url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize a new request: %w", err)
	}

	for k, v := range req.Headers {
		r.Header.Add(k, v)
	}

	return r, nil
}

func convertAuthorizationToken(req *taskspb.HttpRequest) (isAuthorizationToken, error) {
	if req.AuthorizationHeader == nil {
		return nil, nil
	}

	switch h := req.AuthorizationHeader.(type) {
	case *taskspb.HttpRequest_OauthToken:
		return &OAuthToken{
			ServiceAccountEmail: h.OauthToken.ServiceAccountEmail,
			Scope:               h.OauthToken.Scope,
		}, nil

	case *taskspb.HttpRequest_OidcToken:
		return &OIDCToken{
			ServiceAccountEmail: h.OidcToken.ServiceAccountEmail,
			Audience:            h.OidcToken.Audience,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported authorization header type: %s", h)
	}
}

func TaskToPbTask(task *Task) (*taskspb.Task, error) {
	headers := make(map[string]string, len(task.Request.Header))
	for k, v := range task.Request.Header {
		// Headers which can have multiple values (according to RFC2616) can be
		// specified using comma-separated values.
		headers[k] = strings.Join(v, ",")
	}

	var body []byte
	if task.Request.Body != nil {
		var err error
		body, err = io.ReadAll(task.Request.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body: %w", err)
		}
		task.Request.Body = io.NopCloser(bytes.NewReader(body))
	}

	httpRequest := &taskspb.HttpRequest{
		Url:        task.Request.URL.String(),
		HttpMethod: taskspb.HttpMethod(taskspb.HttpMethod_value[task.Request.Method]),
		Headers:    headers,
		Body:       body,
	}

	switch token := task.Authorization.(type) {
	case *OAuthToken:
		httpRequest.AuthorizationHeader = &taskspb.HttpRequest_OauthToken{
			OauthToken: &taskspb.OAuthToken{
				ServiceAccountEmail: token.ServiceAccountEmail,
				Scope:               token.Scope,
			},
		}
	case *OIDCToken:
		httpRequest.AuthorizationHeader = &taskspb.HttpRequest_OidcToken{
			OidcToken: &taskspb.OidcToken{
				ServiceAccountEmail: token.ServiceAccountEmail,
				Audience:            token.Audience,
			},
		}
	}

	return &taskspb.Task{
		Name:         task.TaskName(),
		ScheduleTime: timestamppb.New(task.ScheduledAt),
		MessageType: &taskspb.Task_HttpRequest{
			HttpRequest: httpRequest,
		},
	}, nil
}
