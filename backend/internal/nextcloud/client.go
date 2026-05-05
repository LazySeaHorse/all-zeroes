package nextcloud

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

var httpClient = &http.Client{
	Transport: &http.Transport{
		ResponseHeaderTimeout: 30 * time.Second,
	},
	// No overall timeout: chunk uploads are long-running.
}

// Mkdir creates a directory via WebDAV MKCOL.
// 405 Method Not Allowed means the directory already exists — treated as success.
func Mkdir(ctx context.Context, url, token string) error {
	req, err := http.NewRequestWithContext(ctx, "MKCOL", url, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(token, "")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusMethodNotAllowed {
		return nil
	}
	return fmt.Errorf("MKCOL %s: status %d", url, resp.StatusCode)
}

// Upload streams body to url via PUT with basic auth.
// size must equal the number of bytes in body.
func Upload(ctx context.Context, url, token string, body io.Reader, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return err
	}
	req.SetBasicAuth(token, "")
	req.ContentLength = size

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent ||
		resp.StatusCode == http.StatusOK {
		return nil
	}
	return &StatusError{Code: resp.StatusCode, Op: "PUT", URL: url}
}

// Delete removes a file via WebDAV DELETE.
func Delete(ctx context.Context, url, token string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(token, "")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK ||
		resp.StatusCode == http.StatusNotFound {
		return nil // 404 on delete is fine — already gone
	}
	return fmt.Errorf("DELETE %s: status %d", url, resp.StatusCode)
}

// StatusError carries the HTTP status code so callers can decide whether to retry.
type StatusError struct {
	Code int
	Op   string
	URL  string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s %s: status %d", e.Op, e.URL, e.Code)
}

// IsClientError returns true for 4xx responses (should not be retried).
func IsClientError(err error) bool {
	var se *StatusError
	if e, ok := err.(*StatusError); ok {
		se = e
	}
	return se != nil && se.Code >= 400 && se.Code < 500
}
