package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
)

type RouteConfig struct {
	PathPrefix       string
	MonolithURL      *url.URL
	ServiceURL       *url.URL
	MigrationPercent int // 0–100
	Enabled          bool
}

func NewReverseProxy(target *url.URL) http.Handler {
	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		// optionally update headers, logging, etc.
	}
	return proxy
}

func getEnvURL(key, fallback string) *url.URL {
	val := os.Getenv(key)
	if val == "" {
		val = fallback
	}
	parsed, err := url.Parse(val)
	if err != nil {
		log.Fatalf("Invalid URL for %s: %v", key, err)
	}
	return parsed
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Base backend
	monolithURL := getEnvURL("MONOLITH_URL", "http://localhost:8080")

	gradualMigration := strings.ToLower(os.Getenv("GRADUAL_MIGRATION")) == "true"

	// Define routing configs
	routes := []RouteConfig{
		{
			PathPrefix:       "/api/movies",
			MonolithURL:      monolithURL,
			ServiceURL:       getEnvURL("MOVIES_SERVICE_URL", "http://localhost:8081"),
			MigrationPercent: envPercent("MOVIES_MIGRATION_PERCENT", 0),
			Enabled:          true,
		},
		{
			PathPrefix:       "/api/events",
			MonolithURL:      monolithURL,
			ServiceURL:       getEnvURL("EVENTS_SERVICE_URL", "http://localhost:8082"),
			MigrationPercent: envPercent("EVENTS_MIGRATION_PERCENT", 0),
			Enabled:          true,
		},
	}

	r := chi.NewRouter()

	// health endpoint
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	for _, route := range routes {
		if !route.Enabled {
			continue
		}
		monolithProxy := NewReverseProxy(route.MonolithURL)
		serviceProxy := NewReverseProxy(route.ServiceURL)

		r.Route(route.PathPrefix, func(r chi.Router) {
			r.HandleFunc("/*", func(w http.ResponseWriter, req *http.Request) {
				if !gradualMigration {
					serviceProxy.ServeHTTP(w, req)
					return
				}
				// percent-based switching
				if rand.Intn(100) < route.MigrationPercent {
					serviceProxy.ServeHTTP(w, req)
				} else {
					monolithProxy.ServeHTTP(w, req)
				}
			})
		})
	}

	// fallback: all else go to monolith
	r.NotFound(func(w http.ResponseWriter, req *http.Request) {
		NewReverseProxy(monolithURL).ServeHTTP(w, req)
	})

	log.Println("Proxy starting on :8000")
	if err := http.ListenAndServe(":8000", r); err != nil {
		log.Fatal(err)
	}
}

func envPercent(key string, defVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defVal
	}
	p, err := strconv.Atoi(val)
	if err != nil {
		return defVal
	}
	return p
}
