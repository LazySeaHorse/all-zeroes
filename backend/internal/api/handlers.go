package api

import (
	"encoding/json"
	"net/http"
)

func healthz(scratchDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		free, total := scratchDiskStats(scratchDir)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"status":              "ok",
			"scratch_free_bytes":  free,
			"scratch_total_bytes": total,
		})
	}
}
