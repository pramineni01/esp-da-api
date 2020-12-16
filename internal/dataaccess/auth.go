package dataaccess

import (
	"context"
	"encoding/json"
	"net/http"
)

type userInfo struct {
	Id         string
	Name       string
	GivenName  string
	FamilyName string
	Email      string
	Locale     string
}

// Middleware stores Loaders as a request-scoped context value.
func AuthMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h_user_info := r.Header.Get("user-info")
			var user_info userInfo
			json.Unmarshal([]byte(h_user_info), &user_info)

			ctx := r.Context()
			augmentedCtxA := context.WithValue(ctx, "userId", user_info.Id)
			augmentedCtxB := context.WithValue(augmentedCtxA, "userLocale", user_info.Locale)

			r = r.WithContext(augmentedCtxB)
			next.ServeHTTP(w, r)
		})
	}
}
