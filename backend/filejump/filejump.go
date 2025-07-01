// Package filejump provides an interface to FileJump object storage
package filejump

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
)

// Constants
const (
	apiURL = "https://drive.filejump.com/api/v1"
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "filejump",
		Description: "FileJump",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "encoding",
			Help:     "The encoding for the backend.",
			Default:  encoder.Base,
			Advanced: true,
		}, {
			Name:     "token",
			Help:     "API access token for FileJump.",
			Required: true,
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	Enc   encoder.MultiEncoder `config:"encoding"`
	Token string               `config:"token"`
}

// Fs represents a remote FileJump
type Fs struct {
	name           string         // name of this remote
	root           string         // the path we are working on
	opt            Options        // parsed options
	ci             *fs.ConfigInfo // global config
	features       *fs.Features   // optional features
	client         *http.Client   // the connection to the server
	slashRoot      string         // root with "/" prefix
	slashRootSlash string         // root with "/" prefix and postfix
	pacer          *fs.Pacer      // To pace the API calls
}

// Object describes a FileJump object
type Object struct {
	fs      *Fs    // what this object is part of
	remote  string // The remote path
	id      string // The ID of the object
	bytes   int64  // size of the object
	modTime time.Time
}

// FileEntry represents a file or folder in FileJump
type FileEntry struct {
	ID          int64      `json:"id"`
	Name        string     `json:"name"`
	FileName    string     `json:"file_name"`
	FileSize    int64      `json:"file_size"`
	ParentID    int64      `json:"parent_id"`
	Parent      *FileEntry `json:"parent"`
	Thumbnail   bool       `json:"thumbnail"`
	Mime        string     `json:"mime"`
	URL         string     `json:"url"`
	Hash        string     `json:"hash"`
	Type        string     `json:"type"`
	Description string     `json:"description"`
	DeletedAt   string     `json:"deleted_at"`
	CreatedAt   string     `json:"created_at"`
	UpdatedAt   string     `json:"updated_at"`
	Path        string     `json:"path"`
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	// Create a new Fs
	f := &Fs{
		name: name,
		opt:  *opt,
		ci:   fs.GetConfig(ctx),
	}

	// Set up the pacer
	f.pacer = fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(10*time.Millisecond), pacer.MaxSleep(2*time.Second), pacer.DecayConstant(2)))

	// Set the root
	f.setRoot(root)

	// Create the client
	f.client = &http.Client{}

	// Set up features
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		ReadMimeType:            true,
		WriteMimeType:           true,
	}).Fill(ctx, f)

	return f, nil
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("FileJump root '%s'", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return (&fs.Features{
		CanHaveEmptyDirectories: true,
		ReadMimeType:            true,
		WriteMimeType:           true,
		Copy:                    f.copy,
	}).Fill(context.Background(), f)
}

// getObject gets an object by its remote path
func (f *Fs) getObject(ctx context.Context, remote string) (*Object, error) {
	// List files in the directory
	dir := path.Dir(remote)
	entries, err := f.List(ctx, dir)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory: %w", err)
	}

	// Find the object
	for _, entry := range entries {
		if entry.Remote() == remote {
			if o, ok := entry.(*Object); ok {
				return o, nil
			}
			return nil, fmt.Errorf("entry is not an object: %s", remote)
		}
	}

	return nil, fs.ErrorObjectNotFound
}

// copy an object
func (f *Fs) copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	// Get source object
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fmt.Errorf("invalid source object type")
	}

	// Get destination parent ID
	dstParentID, err := f.getParentID(ctx, path.Dir(remote))
	if err != nil {
		return nil, fmt.Errorf("failed to get destination parent ID: %w", err)
	}

	// Create request body
	body := struct {
		EntryIDs      []string `json:"entryIds"`
		DestinationID int64    `json:"destinationId"`
	}{
		EntryIDs:      []string{srcObj.id},
		DestinationID: dstParentID,
	}

	// Marshal body
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Create request
	url := fmt.Sprintf("%s/file-entries/duplicate", apiURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set content type
	req.Header.Set("Content-Type", "application/json")

	// Make request with retries
	resp, err := f.doRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("bad response from server: %s - %s", resp.Status, string(respBody))
	}

	// Parse response
	var result struct {
		Entries []FileEntry `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if len(result.Entries) == 0 {
		return nil, fmt.Errorf("no entries returned after copy")
	}

	// Create new object
	entry := result.Entries[0]
	modTime, _ := parseTime(entry.UpdatedAt)
	o := &Object{
		fs:      f,
		remote:  remote,
		id:      fmt.Sprintf("%d", entry.ID),
		bytes:   entry.FileSize,
		modTime: modTime,
	}

	return o, nil
}


// setRoot sets the root of the Fs
func (f *Fs) setRoot(root string) {
	f.root = root
	f.slashRoot = "/" + root
	if !strings.HasSuffix(f.slashRoot, "/") {
		f.slashRoot += "/"
	}
	f.slashRootSlash = f.slashRoot
}

// parseTime parses a time string from the API
func parseTime(t string) (time.Time, error) {
	if t == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, t)
}

// shouldRetry determines if an error should be retried
func shouldRetry(err error) (bool, error) {
	if err == nil {
		return false, nil
	}

	// Check for network errors
	if strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "connection reset") ||
		strings.Contains(err.Error(), "connection timed out") ||
		strings.Contains(err.Error(), "no such host") {
		return true, err
	}

	// Check for rate limiting
	if strings.Contains(err.Error(), "rate limit exceeded") ||
		strings.Contains(err.Error(), "too many requests") {
		return true, err
	}

	// Check for server errors
	if strings.Contains(err.Error(), "internal server error") ||
		strings.Contains(err.Error(), "service unavailable") {
		return true, err
	}

	return false, err
}

// doRequest makes an HTTP request with retries
func (f *Fs) doRequest(ctx context.Context, req *http.Request) (*http.Response, error) {
	// Add authorization header
	req.Header.Set("Authorization", "Bearer "+f.opt.Token)
	req.Header.Set("Accept", "application/json")

	var resp *http.Response
	var err error

	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.client.Do(req)
		if err != nil {
			return shouldRetry(err)
		}

		// Check for rate limiting
		if resp.StatusCode == http.StatusTooManyRequests {
			return true, fmt.Errorf("rate limit exceeded")
		}

		// Check for server errors
		if resp.StatusCode >= 500 {
			return true, fmt.Errorf("server error: %s", resp.Status)
		}

		// Check for authentication errors
		if resp.StatusCode == http.StatusUnauthorized {
			return false, fmt.Errorf("authentication failed: invalid or expired token")
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return resp, nil
}

// getParentID gets the parent ID for a path
func (f *Fs) getParentID(ctx context.Context, p string) (int64, error) {
	if p == "" || p == "." {
		return 0, nil
	}

	parts := strings.Split(p, "/")
	parentID := int64(0)

	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}

		var entry *FileEntry
		for i := 0; i < 5; i++ {
			// List entries in the current parent
			url := fmt.Sprintf("%s/drive/file-entries", apiURL)
			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				return 0, fmt.Errorf("failed to create request: %w", err)
			}
			q := req.URL.Query()
			q.Add("parentIds", fmt.Sprintf("%d", parentID))
			req.URL.RawQuery = q.Encode()

			resp, err := f.doRequest(ctx, req)
			if err != nil {
				return 0, fmt.Errorf("failed to make request: %w", err)
			}
			respBody, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return 0, fmt.Errorf("failed to read response body: %w", err)
			}
			fs.Debugf(f, "getParentID response: %s", string(respBody))

			var result struct {
				Data []FileEntry `json:"data"`
			}
			if err := json.Unmarshal(respBody, &result); err != nil {
				return 0, fmt.Errorf("failed to parse response: %w", err)
			}

			found := false
			for _, e := range result.Data {
				if e.Type == "folder" && e.Name == part {
					entry = &e
					found = true
					break
				}
			}
			if found {
				break
			}
			time.Sleep(time.Second)
		}

		if entry == nil {
			return 0, fmt.Errorf("parent folder not found: %s", part)
		}
		parentID = entry.ID
	}
	return parentID, nil
}

// List the objects and directories in dir into entries
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	fs.Debugf(f, "Listing directory: %s", dir)

	// Get parent ID
	parentID, err := f.getParentID(ctx, dir)
	if err != nil {
		return nil, fmt.Errorf("failed to get parent ID: %w", err)
	}
	fs.Debugf(f, "Parent ID: %d", parentID)

	// Make API request to list files
	url := fmt.Sprintf("%s/drive/file-entries", apiURL)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add query parameters
	q := req.URL.Query()
	q.Add("parentIds", fmt.Sprintf("%d", parentID))
	req.URL.RawQuery = q.Encode()
	fs.Debugf(f, "Request URL: %s", req.URL.String())

	// Make request with retries
	resp, err := f.doRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body for debugging
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	fs.Debugf(f, "Response body: %s", string(body))

	// Parse response
	var result struct {
		CurrentPage int         `json:"current_page"`
		Data        []FileEntry `json:"data"`
		From        interface{} `json:"from"`
		NextPage    interface{} `json:"next_page"`
		PerPage     int         `json:"per_page"`
		PrevPage    interface{} `json:"prev_page"`
		To          interface{} `json:"to"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	fs.Debugf(f, "Found %d entries", len(result.Data))

	// Convert to DirEntries
	entries = make(fs.DirEntries, 0, len(result.Data))
	for _, entry := range result.Data {
		// Skip entries that don't belong to the current directory
		if entry.ParentID != parentID {
			continue
		}

		remote := path.Join(dir, entry.Name)
		fs.Debugf(f, "Processing entry: %s (type: %s)", remote, entry.Type)
		if entry.Type == "folder" {
			// Create directory entry
			modTime, _ := parseTime(entry.CreatedAt)
			d := fs.NewDir(remote, modTime)
			entries = append(entries, d)
		} else {
			// Create file entry
			modTime, _ := parseTime(entry.UpdatedAt)
			o := &Object{
				fs:      f,
				remote:  remote,
				id:      fmt.Sprintf("%d", entry.ID),
				bytes:   entry.FileSize,
				modTime: modTime,
			}
			entries = append(entries, o)
		}
	}

	return entries, nil
}

// NewObject finds the Object at remote
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	// List files in the directory
	dir := path.Dir(remote)
	entries, err := f.List(ctx, dir)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory: %w", err)
	}

	// Find the object
	for _, entry := range entries {
		if entry.Remote() == remote {
			if o, ok := entry.(fs.Object); ok {
				return o, nil
			}
			return nil, fmt.Errorf("entry is not an object: %s", remote)
		}
	}

	return nil, fs.ErrorObjectNotFound
}

// Put in to the remote path with the modTime given of the given size
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// Get parent ID
	parentID, err := f.getParentID(ctx, path.Dir(src.Remote()))
	if err != nil {
		// If parent doesn't exist, create it
		err = f.Mkdir(ctx, path.Dir(src.Remote()))
		if err != nil {
			return nil, fmt.Errorf("failed to create parent directory: %w", err)
		}
		parentID, err = f.getParentID(ctx, path.Dir(src.Remote()))
		if err != nil {
			return nil, fmt.Errorf("failed to get parent ID after creating directory: %w", err)
		}
	}

	// Create multipart form data
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add file
	part, err := writer.CreateFormFile("file", src.Remote())
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}

	// Copy file content
	_, err = io.Copy(part, in)
	if err != nil {
		return nil, fmt.Errorf("failed to copy file content: %w", err)
	}

	// Add parentId
	if err := writer.WriteField("parentId", fmt.Sprintf("%d", parentID)); err != nil {
		return nil, fmt.Errorf("failed to write parentId: %w", err)
	}

	// Close writer
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	// Create request
	url := fmt.Sprintf("%s/uploads", apiURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set content type
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// Make request with retries
	resp, err := f.doRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("bad response from server: %s - %s", resp.Status, string(respBody))
	}

	// Parse response
	var result struct {
		Status    string    `json:"status"`
		FileEntry FileEntry `json:"fileEntry"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// Create object
	modTime, _ := parseTime(result.FileEntry.UpdatedAt)
	o := &Object{
		fs:      f,
		remote:  src.Remote(),
		id:      fmt.Sprintf("%d", result.FileEntry.ID),
		bytes:   result.FileEntry.FileSize,
		modTime: modTime,
	}

	return o, nil
}

// Mkdir creates a container if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	// Get parent ID
	parentID, err := f.getParentID(ctx, path.Dir(dir))
	if err != nil {
		return err
	}

	// Get folder name from path
	folderName := path.Base(dir)
	if folderName == "" || folderName == "." {
		folderName = "rclone-folder"
	}

	// Ensure name is at least 3 characters
	if len(folderName) < 3 {
		folderName = "dir_" + folderName
	}

	// Create request body
	reqBody := struct {
		Name     string `json:"name"`
		ParentID int64  `json:"parent_id"`
		Type     string `json:"type"`
	}{
		Name:     folderName,
		ParentID: parentID,
		Type:     "folder",
	}

	// Marshal request body
	reqBodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	fs.Debugf(f, "Mkdir request body: %s", string(reqBodyBytes))

	// Create request
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/folders", apiURL), bytes.NewBuffer(reqBodyBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set content type
	req.Header.Set("Content-Type", "application/json")

	// Make request
	resp, err := f.doRequest(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body for debugging
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	fs.Debugf(f, "Mkdir response: %s", string(respBody))

	// Check response status
	if resp.StatusCode == http.StatusUnprocessableEntity {
		// Check if folder already exists
		var errorResp struct {
			Errors struct {
				Name string `json:"name"`
			} `json:"errors"`
		}
		if err := json.Unmarshal(respBody, &errorResp); err == nil {
			if strings.Contains(errorResp.Errors.Name, "already exists") {
				// Folder already exists, this is not an error
				return nil
			}
		}
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("bad response from server: %s - %s", resp.Status, string(respBody))
	}

	// Parse response
	var result struct {
		Folder struct {
			ID int64 `json:"id"`
		} `json:"folder"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	return nil
}

// Rmdir deletes the container
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	// Get directory ID
	dirID, err := f.getParentID(ctx, dir)
	if err != nil {
		return fmt.Errorf("failed to get directory ID: %w", err)
	}

	// Don't delete root
	if dirID == 0 {
		return nil
	}

	// Create request body
	body := struct {
		EntryIDs      []string `json:"entryIds"`
		DeleteForever bool     `json:"deleteForever"`
	}{
		EntryIDs:      []string{fmt.Sprintf("%d", dirID)},
		DeleteForever: true,
	}

	// Marshal body
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Create request
	url := fmt.Sprintf("%s/file-entries/delete", apiURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set content type
	req.Header.Set("Content-Type", "application/json")

	// Make request with retries
	resp, err := f.doRequest(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bad response from server: %s - %s", resp.Status, string(respBody))
	}

	return nil
}

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Hashes returns the supported hash sets
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// Object methods

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// ModTime returns the modification time of the object
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// SetModTime sets the modification time of the object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	// TODO: Implement setting modification time
	return fs.ErrorNotImplemented
}

// Size returns the size of the object
func (o *Object) Size() int64 {
	return o.bytes
}

// Open opens the file for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	// Get file metadata first
	url := fmt.Sprintf("%s/file-entries/%s", apiURL, o.id)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Make request with retries
	resp, err := o.fs.doRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	// Check response status
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("bad response from server: %s", resp.Status)
	}

	// Parse response
	var result struct {
		Status    string    `json:"status"`
		FileEntry FileEntry `json:"fileEntry"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	resp.Body.Close()

	// Create download request
	url = fmt.Sprintf("%s/%s", apiURL, result.FileEntry.URL)
	req, err = http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create download request: %w", err)
	}

	// Make request with retries
	resp, err = o.fs.doRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to make download request: %w", err)
	}

	// Check response status
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("bad response from server: %s", resp.Status)
	}

	return resp.Body, nil
}

// Update in to the object with the modTime given of the given size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	// Create multipart form data
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add file
	part, err := writer.CreateFormFile("file", src.Remote())
	if err != nil {
		return fmt.Errorf("failed to create form file: %w", err)
	}

	// Copy file content
	_, err = io.Copy(part, in)
	if err != nil {
		return fmt.Errorf("failed to copy file content: %w", err)
	}

	// Close writer
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// Create request
	url := fmt.Sprintf("%s/file-entries/%s", apiURL, o.id)
	req, err := http.NewRequestWithContext(ctx, "PUT", url, body)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set content type
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// Make request with retries
	resp, err := o.fs.doRequest(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad response from server: %s", resp.Status)
	}

	// Parse response
	var result struct {
		Status    string    `json:"status"`
		FileEntry FileEntry `json:"fileEntry"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if result.Status != "success" {
		return fmt.Errorf("unexpected status: %s", result.Status)
	}

	// Update object metadata
	o.bytes = result.FileEntry.FileSize
	o.modTime, _ = parseTime(result.FileEntry.UpdatedAt)

	return nil
}

// Remove this object
func (o *Object) Remove(ctx context.Context) error {
	// Create request
	url := fmt.Sprintf("%s/file-entries/%s", apiURL, o.id)
	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Make request with retries
	resp, err := o.fs.doRequest(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad response from server: %s", resp.Status)
	}

	return nil
}

// String returns a string representation of the object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Hash returns the hash of the object
func (o *Object) Hash(ctx context.Context, r hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Storable returns whether the object is storable
func (o *Object) Storable() bool {
	return true
}
