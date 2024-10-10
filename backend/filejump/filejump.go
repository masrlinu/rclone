package filejump

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	apiBaseURL = "https://drive.filejump.com/api/v1"
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "filejump",
		Description: "FileJump",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "access_token",
			Help:     "You should create an API access token here: https://drive.filejump.com/account-settings",
			Required: true,
			// IsPassword: true,
			// }, {
			// 	Name:     config.ConfigEncoding,
			// 	Help:     config.ConfigEncodingHelp,
			// 	Advanced: true,
			// 	Default: (encoder.Display |
			// 		encoder.EncodeBackSlash |
			// 		encoder.EncodeRightSpace |
			// 		encoder.EncodeInvalidUtf8),
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	AccessToken string `config:"access_token"`
	// Enc         encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote filejump server
type Fs struct {
	name     string
	root     string
	opt      Options
	features *fs.Features
	srv      *rest.Client
	pacer    *fs.Pacer
	dirCache *dircache.DirCache
}

// Object describes a filejump object
type Object struct {
	fs          *Fs
	remote      string
	hasMetaData bool
	size        int64
	modTime     time.Time
	id          string
	mimeType    string
}

// FileEntry represents a file or folder in FileJump
type FileEntry struct {
	ID          int64       `json:"id"`
	Name        string      `json:"name"`
	FileName    string      `json:"file_name"`
	FileSize    int64       `json:"file_size"`
	ParentID    int64       `json:"parent_id"`
	Thumbnail   interface{} `json:"thumbnail"`
	Mime        string      `json:"mime"`
	URL         string      `json:"url"`
	Hash        string      `json:"hash"`
	Type        string      `json:"type"`
	Description string      `json:"description"`
	DeletedAt   time.Time   `json:"deleted_at"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
	Path        string      `json:"path"`
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	root = strings.Trim(root, "/")

	client := fshttp.NewClient(ctx)

	f := &Fs{
		name:  name,
		root:  root,
		opt:   *opt,
		srv:   rest.NewClient(client).SetRoot(apiBaseURL),
		pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(10*time.Millisecond), pacer.MaxSleep(2*time.Second), pacer.DecayConstant(2))),
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)
	f.srv.SetHeader("Authorization", "Bearer "+opt.AccessToken)

	f.dirCache = dircache.New(root, "0", f)

	// Find the current root
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		// Assume it's a file
		newRoot, remote := dircache.SplitPath(root)
		tempF := *f
		tempF.dirCache = dircache.New(newRoot, "0", &tempF)
		tempF.root = newRoot
		// Make new Fs which is the parent
		err = tempF.dirCache.FindRoot(ctx, false)
		if err != nil {
			// No root so return old f
			return f, nil
		}
		_, err := tempF.newObjectWithInfo(ctx, remote, nil)
		if err != nil {
			if err == fs.ErrorObjectNotFound {
				// File doesn't exist so return old f
				return f, nil
			}
			return nil, err
		}
		// XXX: update the old f here instead of returning tempF, since
		// `features` were already filled with functions having *f as a receiver.
		f.dirCache = tempF.dirCache
		f.root = tempF.root
		return f, fs.ErrorIsFile
	}
	return f, nil
}

// type Fs interface:

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	// fmt.Printf("List called with dir: %s\n", dir)

	directoryID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		// fmt.Printf("Error finding directory ID for dir %s: %v\n", dir, err)
		return nil, err
	}
	// fmt.Printf("Found directory ID: %s\n", directoryID)

	opts := rest.Opts{
		Method: "GET",
		Path:   "/drive/file-entries",
	}

	values := url.Values{}
	values.Set("parentIds", directoryID)
	values.Set("perPage", "1000")
	opts.Parameters = values

	// Log the full request URL
	// requestURL := apiBaseURL + opts.Path + "?" + values.Encode()
	// fmt.Printf("Full API Request URL:\n%s\n", requestURL)

	var result struct {
		Data []FileEntry `json:"data"`
	}

	var resp *http.Response
	var body []byte
	err = f.pacer.Call(func() (bool, error) {
		// fmt.Println("Making API call...")
		resp, err = f.srv.Call(ctx, &opts)
		if err != nil {
			// fmt.Printf("Error during API call: %v\n", err)
			return shouldRetry(ctx, resp, err)
		}

		// Read and log the full response body
		body, err = io.ReadAll(resp.Body)
		if err != nil {
			// fmt.Printf("Error reading response body: %v\n", err)
			return shouldRetry(ctx, resp, err)
		}
		// fmt.Printf("Full API Response:\n%s\n", string(body))

		// Parse the JSON response
		err = json.Unmarshal(body, &result)
		if err != nil {
			// fmt.Printf("Error parsing JSON response: %v\n", err)
			return shouldRetry(ctx, resp, err)
		}

		return false, nil
	})

	if err != nil {
		// fmt.Printf("Error after API call: %v\n", err)
		return nil, err
	}

	// fmt.Printf("API call successful, received %d entries\n", len(result.Data))

	for _, item := range result.Data {
		remote := item.Name
		// fmt.Printf("Processing item: %s, type: %s\n", remote, item.Type)
		if item.Type == "folder" {
			sId := strconv.FormatInt(item.ID, 10)
			f.dirCache.Put(remote, sId)
			d := fs.NewDir(remote, time.Time(item.UpdatedAt)).SetID(sId).SetSize(item.FileSize).SetParentID(strconv.FormatInt(item.ParentID, 10))
			entries = append(entries, d)
			// fmt.Printf("Added directory: %s\n", remote)
		} else {
			o, err := f.newObjectWithInfo(ctx, remote, &item)
			if err != nil {
				// fmt.Printf("Error creating object for %s: %v\n", remote, err)
				continue
			}
			entries = append(entries, o)
			// fmt.Printf("Added file: %s\n", remote)
		}
	}

	// fmt.Printf("Returning %d entries\n", len(entries))
	return entries, nil
}

// Helper-Funktion zur Überprüfung, ob ein Retry notwendig ist
func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if err != nil {
		if fserrors.ShouldRetry(err) {
			return true, err
		}
	} else if resp != nil {
		switch resp.StatusCode {
		case 429, 500, 502, 503, 504:
			return true, fmt.Errorf("got status code %d", resp.StatusCode)
		}
	}
	return false, err
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.newObjectWithInfo(ctx, remote, nil)
}

// Put in to the remote path with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Put should either
// return an error or upload it properly (rather than e.g. calling panic).
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	fs.Logf(nil, "Put wurde aufgerufen")
	return nil, fs.ErrorNotImplemented
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	fs.Logf(nil, "Mkdir wurde aufgerufen für Verzeichnis: %s", dir)
	return fs.ErrorNotImplemented
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	fs.Logf(nil, "Rmdir wurde aufgerufen für Verzeichnis: %s", dir)
	return fs.ErrorNotImplemented
}

// type Info interface:
// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String returns a description of the FS
func (f *Fs) String() string {
	return "filejump root '" + f.root + "'"
}

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Returns the supported hash types of the filesystem
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	// Erstelle die API-Anfrage
	opts := rest.Opts{
		Method: "GET",
		Path:   "/drive/file-entries",
	}

	// Setze die Query-Parameter
	values := url.Values{}
	values.Set("parentIds", pathID)
	values.Set("query", leaf)
	values.Set("type", "folder")
	opts.Parameters = values

	var result struct {
		Data []FileEntry `json:"data"`
	}

	// Führe die API-Anfrage aus
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &result)
		return shouldRetry(ctx, resp, err)
	})

	if err != nil {
		return "", false, err
	}

	// Überprüfe, ob ein passendes Verzeichnis gefunden wurde
	for _, entry := range result.Data {
		if entry.Name == leaf && entry.Type == "folder" {
			return fmt.Sprintf("%d", entry.ID), true, nil
		}
	}

	// Wenn kein passendes Verzeichnis gefunden wurde
	return "", false, nil
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (newID string, err error) {
	fs.Logf(nil, "CreateDir wurde aufgerufen")
	return "", fs.ErrorNotImplemented
}

func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *FileEntry) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}

	if info != nil {
		// Wenn Informationen bereitgestellt wurden, verwenden wir diese
		err := o.setMetaData(info)
		if err != nil {
			return nil, err
		}
	} else {
		// Andernfalls müssen wir die Informationen vom Server abrufen
		err := o.readMetaData(ctx)
		if err != nil {
			return nil, err
		}
	}

	return o, nil
}

// setMetaData setzt die Metadaten des Objekts basierend auf den FileEntry-Informationen
func (o *Object) setMetaData(info *FileEntry) error {
	o.hasMetaData = true
	o.size = info.FileSize
	o.modTime = info.UpdatedAt
	o.id = fmt.Sprintf("%d", info.ID)
	o.mimeType = info.Mime
	return nil
}

// readMetaData liest die Metadaten des Objekts vom Server
func (o *Object) readMetaData(ctx context.Context) error {
	path := "/drive/file-entries"
	opts := rest.Opts{
		Method: "GET",
		Path:   path,
	}

	query := url.Values{}
	query.Set("query", o.remote)
	opts.Parameters = query

	var result struct {
		Data []FileEntry `json:"data"`
	}

	var resp *http.Response
	var err error
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srv.CallJSON(ctx, &opts, nil, &result)
		return shouldRetry(ctx, resp, err)
	})

	if err != nil {
		return err
	}

	if len(result.Data) == 0 {
		return fs.ErrorObjectNotFound
	}

	return o.setMetaData(&result.Data[0])
}

// Check the interfaces are satisfied
var (
	_ fs.Fs     = (*Fs)(nil)
	_ fs.Object = (*Object)(nil)
)

// type Object interface:

// SetModTime sets the metadata on the object to set the modification date
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	fs.Logf(nil, "SetModTime wurde aufgerufen")
	return fs.ErrorNotImplemented
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.Logf(nil, "Open wurde aufgerufen")
	return nil, fs.ErrorNotImplemented
}

// Update in to the object with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should either
// return an error or update the object properly (rather than e.g. calling panic).
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	fs.Logf(nil, "Update wurde aufgerufen")
	return fs.ErrorNotImplemented
}

// Removes this object
func (o *Object) Remove(ctx context.Context) error {
	fs.Logf(nil, "Remove wurde aufgerufen")
	return fs.ErrorNotImplemented
}

// type ObjectInfo interface:

// Fs returns read only access to the Fs that this object is part of
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Hash returns the selected checksum of the file
// If no checksum is available it returns ""
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Storable says whether this object can be stored
func (o *Object) Storable() bool {
	return true
}

// type DirEntry interface:
// String returns a description of the Object
func (o *Object) String() string {
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// ModTime returns the modification date of the file
// It should return a best guess if one isn't available
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// Size returns the size of the file
func (o *Object) Size() int64 {
	return o.size
}
