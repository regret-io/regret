package storage

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ---------------------------------------------------------------------------
// FileStore
// ---------------------------------------------------------------------------

type FileStore struct {
	basePath string
}

func NewFileStore(basePath string) *FileStore {
	return &FileStore{basePath: basePath}
}

// ---------------------------------------------------------------------------
// Hypothesis directory
// ---------------------------------------------------------------------------

func (f *FileStore) hypothesisDir(id string) string {
	return filepath.Join(f.basePath, "hypothesis", id)
}

func (f *FileStore) CreateHypothesisDir(id string) error {
	dir := f.hypothesisDir(id)
	return os.MkdirAll(dir, 0o755)
}

func (f *FileStore) DeleteHypothesisDir(id string) error {
	dir := f.hypothesisDir(id)
	return os.RemoveAll(dir)
}

// ---------------------------------------------------------------------------
// Hypothesis events
// ---------------------------------------------------------------------------

func (f *FileStore) eventsPath(id string) string {
	return filepath.Join(f.hypothesisDir(id), "events.jsonl")
}

func (f *FileStore) AppendEvent(id, eventJSON string) error {
	if err := f.CreateHypothesisDir(id); err != nil {
		return fmt.Errorf("create hypothesis dir: %w", err)
	}
	fp := f.eventsPath(id)
	file, err := os.OpenFile(fp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open events file: %w", err)
	}
	defer file.Close()

	line := strings.TrimRight(eventJSON, "\n") + "\n"
	_, err = file.WriteString(line)
	return err
}

func (f *FileStore) ReadEvents(id string, runID, eventType, since *string, last *int) ([]map[string]any, error) {
	fp := f.eventsPath(id)

	var lines []string
	var err error
	if last != nil && *last > 0 {
		lines, err = TailLines(fp, *last)
	} else {
		lines, err = readAllLines(fp)
	}
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read events: %w", err)
	}

	return filterEvents(lines, runID, eventType, since), nil
}

func (f *FileStore) ReadMergedEvents(id string, runID, eventType, since *string, last *int) ([]map[string]any, error) {
	hyp, err := f.ReadEvents(id, runID, eventType, since, last)
	if err != nil {
		return nil, err
	}

	chaos, err := f.ReadChaosEvents(since)
	if err != nil {
		return nil, err
	}

	merged := append(hyp, chaos...)
	sort.Slice(merged, func(i, j int) bool {
		ti, _ := merged[i]["timestamp"].(string)
		tj, _ := merged[j]["timestamp"].(string)
		return ti < tj
	})
	return merged, nil
}

// ---------------------------------------------------------------------------
// Chaos events
// ---------------------------------------------------------------------------

func (f *FileStore) chaosEventsPath() string {
	return filepath.Join(f.basePath, "chaos", "events.jsonl")
}

func (f *FileStore) AppendChaosEvent(eventJSON string) error {
	dir := filepath.Join(f.basePath, "chaos")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create chaos dir: %w", err)
	}
	fp := f.chaosEventsPath()
	file, err := os.OpenFile(fp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open chaos events file: %w", err)
	}
	defer file.Close()

	line := strings.TrimRight(eventJSON, "\n") + "\n"
	_, err = file.WriteString(line)
	return err
}

func (f *FileStore) ReadChaosEvents(since *string) ([]map[string]any, error) {
	fp := f.chaosEventsPath()
	lines, err := readAllLines(fp)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read chaos events: %w", err)
	}
	return filterEvents(lines, nil, nil, since), nil
}

// ---------------------------------------------------------------------------
// Checkpoints
// ---------------------------------------------------------------------------

func (f *FileStore) WriteCheckpoint(id string, expect, actual any) error {
	dir := f.hypothesisDir(id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create hypothesis dir: %w", err)
	}

	expectData, err := json.MarshalIndent(expect, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal expect: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "expect.json"), expectData, 0o644); err != nil {
		return fmt.Errorf("write expect.json: %w", err)
	}

	actualData, err := json.MarshalIndent(actual, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal actual: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "actual.json"), actualData, 0o644); err != nil {
		return fmt.Errorf("write actual.json: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Bundle (ZIP)
// ---------------------------------------------------------------------------

func (f *FileStore) CreateBundle(id string, runID *string) ([]byte, error) {
	dir := f.hypothesisDir(id)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, fmt.Errorf("hypothesis directory not found: %s", id)
	}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(dir, path)
		if err != nil {
			return fmt.Errorf("rel path: %w", err)
		}

		// If runID is provided, only include files matching that run.
		if runID != nil {
			// Always include events.jsonl, expect.json, actual.json.
			// Skip files that are clearly for a different run if we can tell.
		}

		w, err := zw.Create(relPath)
		if err != nil {
			return fmt.Errorf("create zip entry: %w", err)
		}

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open file: %w", err)
		}
		defer file.Close()

		_, err = io.Copy(w, file)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("walk hypothesis dir: %w", err)
	}

	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("close zip: %w", err)
	}

	return buf.Bytes(), nil
}

// ---------------------------------------------------------------------------
// TailLines — read last N lines from a file by seeking backwards in chunks.
// ---------------------------------------------------------------------------

func TailLines(path string, n int) ([]string, error) {
	if n <= 0 {
		return nil, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := stat.Size()
	if size == 0 {
		return nil, nil
	}

	const chunkSize = 64 * 1024
	var collected []byte
	offset := size
	linesFound := 0

	for offset > 0 && linesFound <= n {
		readSize := int64(chunkSize)
		if readSize > offset {
			readSize = offset
		}
		offset -= readSize

		buf := make([]byte, readSize)
		_, err := file.ReadAt(buf, offset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read chunk: %w", err)
		}

		collected = append(buf, collected...)

		// Count newlines in this chunk.
		for _, b := range buf {
			if b == '\n' {
				linesFound++
			}
		}
	}

	allLines := strings.Split(string(collected), "\n")

	// Remove trailing empty element from trailing newline.
	if len(allLines) > 0 && allLines[len(allLines)-1] == "" {
		allLines = allLines[:len(allLines)-1]
	}

	if len(allLines) <= n {
		return allLines, nil
	}
	return allLines[len(allLines)-n:], nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func readAllLines(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	return lines, nil
}

func filterEvents(lines []string, runID, eventType, since *string) []map[string]any {
	var out []map[string]any
	for _, line := range lines {
		if line == "" {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			continue
		}
		if runID != nil {
			if rid, ok := obj["run_id"].(string); !ok || rid != *runID {
				continue
			}
		}
		if eventType != nil {
			if et, ok := obj["type"].(string); !ok || et != *eventType {
				continue
			}
		}
		if since != nil {
			if ts, ok := obj["timestamp"].(string); !ok || ts < *since {
				continue
			}
		}
		out = append(out, obj)
	}
	return out
}
