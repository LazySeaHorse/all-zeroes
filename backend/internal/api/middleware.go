package api

import (
	"context"
	"database/sql"
	"net/http"
	"strings"

	"allzeroes/internal/db"
)

type contextKey int

const ctxUser contextKey = 0

func authenticate(database *sql.DB) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, ok := bearerToken(r)
			if !ok {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			user, err := db.GetUserByKey(database, token)
			if err != nil || user == nil {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			ctx := context.WithValue(r.Context(), ctxUser, user)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func currentUser(r *http.Request) *db.User {
	u, _ := r.Context().Value(ctxUser).(*db.User)
	return u
}

func bearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return "", false
	}
	tok := strings.TrimPrefix(h, "Bearer ")
	if tok == "" {
		return "", false
	}
	return tok, true
}
