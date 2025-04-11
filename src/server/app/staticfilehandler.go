package app

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/rs/zerolog/log"
)

// StaticFilesHandler implements http.Handler interface
type StaticFilesHandler struct {
	staticPath       string
	indexFileBuffer  []byte
	indexFileModTime time.Time
}

var customCodeRe = regexp.MustCompile(`CUSTOM_CODE`)

// NewStaticFilesHandler creates a new StaticFilesHandler
func NewStaticFilesHandler(staticPath string) StaticFilesHandler {
	fs := http.Dir(staticPath)
	indexFile, err := fs.Open("./index.html")
	if err != nil {
		log.Fatal().Str("DEKART_STATIC_FILES", staticPath).Err(err).Msg("Failed to open index.html file")
	}
	defer indexFile.Close()
	stat, err := indexFile.Stat()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get index.html file info")
	}
	template := make([]byte, stat.Size())
	_, err = indexFile.Read(template)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to read index.html file")
	}
	indexFileBuffer := customCodeRe.ReplaceAll(template, []byte(os.Getenv("DEKART_HTML_CUSTOM_CODE")))

	staticFilesHandler := StaticFilesHandler{
		staticPath:       staticPath,
		indexFileBuffer:  indexFileBuffer,
		indexFileModTime: time.Now(),
	}
	return staticFilesHandler
}

// ServeHTTP implementation for reading static files from build folder
func (h StaticFilesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := filepath.Clean(r.URL.Path)
	path = filepath.Join(h.staticPath, path)
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		h.ServeIndex(ResponseWriter{w: w, statusCode: http.StatusNotFound}, r)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.FileServer(http.Dir(h.staticPath)).ServeHTTP(w, r)
}

// ServeIndex serves index.html
func (h StaticFilesHandler) ServeIndex(w http.ResponseWriter, r *http.Request) {
	// Set caching headers for index.html
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate") // Prevent caching
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	http.ServeContent(w, r, "index.html", h.indexFileModTime, bytes.NewReader(h.indexFileBuffer))
}
