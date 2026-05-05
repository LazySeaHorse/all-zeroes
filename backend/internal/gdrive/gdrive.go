package gdrive

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

// reTransferred matches rclone --progress lines like:
//
//	Transferred:      123.456 MiB / 1.500 GiB, 8%, 12.345 MiB/s, ETA 2m3s
var reTransferred = regexp.MustCompile(`Transferred:\s+([\d.]+)\s+(\w+)\s+/`)

// unitBytes converts an rclone size unit string to bytes.
func unitBytes(n float64, unit string) int64 {
	switch strings.ToUpper(unit) {
	case "B":
		return int64(n)
	case "KIB":
		return int64(n * 1024)
	case "MIB":
		return int64(n * 1024 * 1024)
	case "GIB":
		return int64(n * 1024 * 1024 * 1024)
	case "TIB":
		return int64(n * 1024 * 1024 * 1024 * 1024)
	// rclone also prints KB / MB / GB (decimal) in some builds
	case "KB":
		return int64(n * 1000)
	case "MB":
		return int64(n * 1000 * 1000)
	case "GB":
		return int64(n * 1000 * 1000 * 1000)
	case "TB":
		return int64(n * 1000 * 1000 * 1000 * 1000)
	default:
		return int64(n)
	}
}

// ProgressFunc is called each time rclone reports a transferred-bytes update.
// transferred is the number of bytes transferred so far (absolute, not a delta).
type ProgressFunc func(transferred int64)

// Copy runs `rclone copy --progress src dst` and calls onProgress with
// the absolute byte count each time rclone emits a progress line.
// It blocks until rclone exits.
func Copy(ctx context.Context, src, dst string, onProgress ProgressFunc) error {
	cmd := exec.CommandContext(ctx, "rclone", "copy", "--progress", src, dst)

	// rclone writes progress to stderr.
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("rclone pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("rclone start: %w", err)
	}

	// Read progress from stderr in a goroutine.
	go parseProgress(stderr, onProgress)

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("rclone: %w", err)
	}
	return nil
}

func parseProgress(r io.Reader, onProgress ProgressFunc) {
	// rclone overwrites the same line using \r; split on both \n and \r.
	scanner := bufio.NewScanner(r)
	scanner.Split(scanLines)
	for scanner.Scan() {
		line := scanner.Text()
		m := reTransferred.FindStringSubmatch(line)
		if len(m) < 3 {
			continue
		}
		n, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			continue
		}
		transferred := unitBytes(n, m[2])
		if onProgress != nil {
			onProgress(transferred)
		}
	}
	if err := scanner.Err(); err != nil {
		slog.Debug("rclone stderr scanner", "err", err)
	}
}

// scanLines is a bufio.SplitFunc that splits on \n or \r (for rclone's
// carriage-return-based progress updates).
func scanLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for i, b := range data {
		if b == '\n' || b == '\r' {
			return i + 1, data[:i], nil
		}
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}
