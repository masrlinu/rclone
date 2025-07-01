package filejump

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/backend/filejump/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	apiBaseURL          = "https://drive.filejump.com/api/v1"
	defaultUploadCutoff = 50 * 1024 * 1024
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
		}, {
			Name:     "upload_cutoff",
			Help:     "Cutoff for switching to multipart upload (>= 50 MiB).",
			Default:  fs.SizeSuffix(defaultUploadCutoff),
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			// From https://developer.box.com/docs/error-codes#section-400-bad-request :
			// > Box only supports file or folder names that are 255 characters or less.
			// > File names containing non-printable ascii, "/" or "\", names with leading
			// > or trailing spaces, and the special names “.” and “..” are also unsupported.
			//
			// Testing revealed names with leading spaces work fine.
			// Also encode invalid UTF-8 bytes as json doesn't handle them properly.
			Default: (encoder.Display |
				encoder.EncodeBackSlash |
				encoder.EncodeRightSpace |
				encoder.EncodeInvalidUtf8),
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	UploadCutoff fs.SizeSuffix `config:"upload_cutoff"`
	// CommitRetries int                  `config:"commit_retries"`
	Enc encoder.MultiEncoder `config:"encoding"`
	// RootFolderID  string               `config:"root_folder_id"`
	AccessToken string `config:"access_token"`
	// ListChunk     int                  `config:"list_chunk"`
	// OwnedBy       string               `config:"owned_by"`
	// Impersonate   string               `config:"impersonate"`
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

// callJSON ist eine generische Funktion für API-Aufrufe
/*
POST-Request Template:
--------------------
type Request struct {
    Field1 string `json:"field1"`
    Field2 int    `json:"field2"`
}
request := Request{
    Field1: "wert1",
    Field2: 42,
}

type Response struct {
    Status string `json:"status"`
    Data   string `json:"data,omitempty"`
}

result, err := CallJSON[Response, Request](f, ctx, "POST", "/api/endpoint", nil, &request)
if err != nil {
    return err
}

GET-Request Template:
-------------------
type Response struct {
    Status string `json:"status"`
    Data   string `json:"data,omitempty"`
}
values := url.Values{}
values.Set("param1", "wert1")
values.Set("param2", "wert2")
values.Set("EntryIds", fmt.Sprintf("[%s]", dir))
values.Set("DeleteForever", strconv.FormatBool(true)))

result, err := CallJSON[Response, struct{}](f, ctx, "GET", "/file-entries/delete", &values, nil)
if err != nil {
    return err
}
*/
func CallJSON[T any, B any](f *Fs, ctx context.Context, method string, path string, params *url.Values, body *B) (*T, error) {
	// Eingabeparameter in einer Zeile
	logMsg := fmt.Sprintf("CallJSON: %s %s", method, path)
	if params != nil && len(*params) > 0 {
		logMsg += fmt.Sprintf(" params=%s", params.Encode())
	}
	if body != nil {
		bodyJSON, _ := json.Marshal(body)
		logMsg += fmt.Sprintf(" body=%s", string(bodyJSON))
	}
	fs.Logf(nil, logMsg)

	var result T
	opts := rest.Opts{
		Method: method,
		Path:   path,
		ExtraHeaders: map[string]string{
			"Accept":          "application/json",
			"Accept-Encoding": "gzip, deflate, br, zstd",
		},
	}

	if params != nil {
		opts.Parameters = *params
	}

	err := f.pacer.Call(func() (bool, error) {
		resp, err := f.srv.CallJSON(ctx, &opts, body, &result)

		// Nur Status loggen wenn nicht OK
		if resp != nil && resp.StatusCode != http.StatusOK {
			fs.Logf(nil, "CallJSON: Response Status: %s (%d)", resp.Status, resp.StatusCode)
		}

		// Nur bei Erfolg und nicht-leerem Body loggen
		if err == nil {
			resultJSON, _ := json.Marshal(result)
			if string(resultJSON) != "{}" && string(resultJSON) != "null" {
				fs.Logf(nil, "CallJSON: Response Body: %s", string(resultJSON))
			}
		}

		return shouldRetry(ctx, resp, err)
	})

	if err != nil {
		fs.Logf(nil, "CallJSON: Error: %v", err)
		return nil, err
	}

	return &result, nil
}

// CallJSONGet führt einen GET-Request aus
/*
Beispielaufruf:
--------------
type Response struct {
    Status string `json:"status"`
    Data   string `json:"data,omitempty"`
}

values := url.Values{}
values.Set("param1", "wert1")
values.Set("param2", "wert2")

result, err := CallJSONGet[Response](f, ctx, "/api/endpoint", &values)
if err != nil {
    return err
}
*/
func CallJSONGet[T any](f *Fs, ctx context.Context, path string, params *url.Values) (*T, error) {
	return CallJSON[T, struct{}](f, ctx, "GET", path, params, nil)
}

// CallJSONPost führt einen POST-Request aus
/*
Beispielaufruf:
--------------
type RequestDelete struct {
	EntryIds      []int `json:"entryIds"`
	DeleteForever bool  `json:"deleteForever"`
}

type ResultDelete struct {
	Status string `json:"status,omitempty"`
}

iDir, _ := strconv.Atoi(dir)
request := RequestDelete{
	EntryIds:      []int{iDir},
	DeleteForever: true,
}

result, err := CallJSONPost[ResultDelete, RequestDelete](f, ctx, "/file-entries/delete", &request)
if err != nil {
    return err
}
*/
func CallJSONPost[T any, B any](f *Fs, ctx context.Context, path string, body *B) (*T, error) {
	return CallJSON[T, B](f, ctx, "POST", path, &url.Values{}, body)
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	fs.Logf(nil, "NewFs: Initialisiere neues Filesystem mit name='%s', root='%s'", name, root)
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

	var rootID string
	f.dirCache = dircache.New(root, rootID, f)

	// Find the current root
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		// Assume it's a file
		newRoot, remote := dircache.SplitPath(root)
		tempF := *f
		tempF.dirCache = dircache.New(newRoot, rootID, &tempF)
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

// list the objects into the function supplied
//
// If directories is set it only sends directories
// User function to process a File item from listAll
//
// Should return true to finish processing
type listAllFn func(*api.Item) bool

// Lists the directory required calling the user function on each item found
//
// If the user fn ever returns true then it early exits with found = true
func (f *Fs) listAll(ctx context.Context, dirID string, directoriesOnly bool, filesOnly bool, activeOnly bool, fn listAllFn) (found bool, err error) {
	fs.Logf(nil, "listAll wurde aufgerufen")
	values := url.Values{}
	values.Set("folderId", dirID)
	// values.Set("parentIds", dirID)
	values.Set("perPage", "1000")
	// section=home
	// folderId=0
	// workspaceId=0
	// orderBy=updated_at
	// orderDir=desc
	// page=1

	var page *uint
OUTER:
	for {
		if page != nil {
			values.Set("page", strconv.FormatUint(uint64(*page), 10))
		}

		result, err := CallJSONGet[api.FileEntries](f, ctx, "/drive/file-entries", &values)
		if err != nil {
			return found, fmt.Errorf("couldn't list files: %w", err)
		}
		for i := range result.Data {
			item := &result.Data[i]
			if item.Type == api.ItemTypeFolder {
				if filesOnly {
					continue
				}
			} else if item.Type != api.ItemTypeFolder {
				if directoriesOnly {
					continue
				}
			} else {
				fs.Debugf(f, "Ignoring %q - unknown type %q", item.Name, item.Type)
				continue
			}
			// At the moment, there is no trash at FileJump
			// if activeOnly && item.ItemStatus != api.ItemStatusActive {
			// 	continue
			// }
			// if f.opt.OwnedBy != "" && f.opt.OwnedBy != item.OwnedBy.Login {
			// 	continue
			// }
			item.Name = f.opt.Enc.ToStandardName(item.Name)
			if fn(item) {
				found = true
				break OUTER
			}
		}
		page = result.NextPage
		if page == nil {
			break
		}
	}
	return
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
	fs.Logf(nil, "List: Verzeichnis '%s' wird aufgelistet", dir)
	directoryID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	var iErr error
	_, err = f.listAll(ctx, directoryID, false, false, true, func(info *api.Item) bool {
		remote := path.Join(dir, info.Name)
		if info.Type == api.ItemTypeFolder {
			// cache the directory ID for later lookups
			f.dirCache.Put(remote, info.GetID())
			d := fs.NewDir(remote, info.ModTime()).SetID(info.GetID())
			// FIXME more info from dir?
			entries = append(entries, d)
		} else if info.Type != api.ItemTypeFolder {
			o, err := f.newObjectWithInfo(ctx, remote, info)
			if err != nil {
				iErr = err
				return true
			}
			entries = append(entries, o)
		}

		// // Cache some metadata for this Item to help us process events later
		// // on. In particular, the box event API does not provide the old path
		// // of the Item when it is renamed/deleted/moved/etc.
		// f.itemMetaCacheMu.Lock()
		// cachedItemMeta, found := f.itemMetaCache[info.GetID()]
		// if !found || cachedItemMeta.SequenceID < info.SequenceID {
		// 	f.itemMetaCache[info.ID] = ItemMeta{SequenceID: info.SequenceID, ParentID: directoryID, Name: info.Name}
		// }
		// f.itemMetaCacheMu.Unlock()

		return false
	})
	if err != nil {
		return nil, err
	}
	if iErr != nil {
		return nil, iErr
	}
	return entries, nil
}

// retryErrorCodes is a slice of error codes that we will retry
var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried.  It returns the err as a convenience
func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	authRetry := false

	if resp != nil && resp.StatusCode == 401 && strings.Contains(resp.Header.Get("Www-Authenticate"), "expired_token") {
		authRetry = true
		fs.Debugf(nil, "Should retry: %v", err)
	}

	// // FileJump API errors which should be retried
	// if apiErr, ok := err.(*api.Error); ok && apiErr.Code == "operation_blocked_temporary" {
	// 	fs.Debugf(nil, "Retrying API error %v", err)
	// 	return true, err
	// }

	return authRetry || fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fs.Logf(nil, "NewObject wurde aufgerufen")
	return f.newObjectWithInfo(ctx, remote, nil)
}

// Creates from the parameters passed in a half finished Object which
// must have setMetaData called on it
//
// Returns the object, leaf, directoryID and error.
//
// Used to create new objects
func (f *Fs) createObject(ctx context.Context, remote string, modTime time.Time, size int64) (o *Object, leaf string, directoryID string, err error) {
	fs.Logf(nil, "createObject wurde aufgerufen")
	// Create the directory for the object if it doesn't exist
	leaf, directoryID, err = f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return
	}
	// Temporary Object under construction
	o = &Object{
		fs:     f,
		remote: remote,
	}
	return o, leaf, directoryID, nil
}

// PutUnchecked the object into the container
//
// This will produce an error if the object already exists.
//
// Copy the reader in to the new object which is returned.
//
// The new object may have been created if an error is returned
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	fs.Logf(nil, "PutUnchecked wurde aufgerufen")
	remote := src.Remote()
	size := src.Size()
	modTime := src.ModTime(ctx)

	o, _, _, err := f.createObject(ctx, remote, modTime, size)
	if err != nil {
		return nil, err
	}
	return o, o.Update(ctx, in, src, options...)
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

// Put the object into the container
//
// Copy the reader in to the new object which is returned.
//
// The new object may have been created if an error is returned
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	fs.Logf(nil, "Put wurde aufgerufen")
	remote := src.Remote()
	size := src.Size()
	modTime := src.ModTime(ctx)

	o, _, _, err := f.createObject(ctx, remote, modTime, size)
	if err != nil {
		return nil, err
	}
	return o, o.Update(ctx, in, src, options...)
}

// // Mkdir makes the directory (container, bucket)
// //
// // Shouldn't return an error if it already exists
// func (f *Fs) Mkdir(ctx context.Context, dir string) error {
// 	presignResult, err := CallJSON[PresignResult](f, ctx, "POST", "/folders", url.Values{
// 		"name": []string{dir}
// :
// "test123"
// parentId
// :
// 		"Filename":     []string{leaf},
// 		"Mime":         []string{"application/octet-stream"},
// 		"Disk":         []string{"uploads"},
// 		"Size":         []string{strconv.FormatInt(size, 10)},
// 		"Extension":    []string{"bin"},
// 		"WorkspaceID":  []string{"0"},
// 		"ParentID":     []string{directoryID},
// 		"RelativePath": []string{""},
// 	})

// 	if err != nil {
// 		// Fehlerbehandlung
// 	}
// }

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	fs.Logf(nil, "Mkdir wurde aufgerufen")
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// purgeCheck removes the root directory, if check is set then it
// refuses to do so if it has anything in
func (f *Fs) purgeCheck(ctx context.Context, dir string, check bool) error {
	fs.Logf(nil, "purgeCheck wurde aufgerufen")
	root := path.Join(f.root, dir)
	if root == "" {
		return errors.New("can't purge root directory")
	}
	dc := f.dirCache
	rootID, err := dc.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}

	type RequestDelete struct {
		EntryIds      []int `json:"entryIds"`
		DeleteForever bool  `json:"deleteForever"`
	}

	type ResultDelete struct {
		Status string `json:"status,omitempty"`
	}

	iDir, _ := strconv.Atoi(rootID)
	request := RequestDelete{
		EntryIds:      []int{iDir},
		DeleteForever: true,
	}

	result, err := CallJSONPost[ResultDelete, RequestDelete](f, ctx, "/file-entries/delete", &request)

	if err != nil {
		return fmt.Errorf("rmdir failed: %w", err)
	}
	if result.Status != "success" {
		return errors.New("delete, no api success")
	}
	f.dirCache.FlushDir(dir)
	if err != nil {
		return errors.New("rmdir failed, no success response")
	}
	return nil
}

// Rmdir deletes the root folder
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	fs.Logf(nil, "Rmdir wurde aufgerufen")
	return f.purgeCheck(ctx, dir, true)
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
	fs.Logf(nil, "FindLeaf: Suche nach Blatt '%s' in Verzeichnis mit ID '%s'", leaf, pathID)
	// Find the leaf in pathID
	found, err = f.listAll(ctx, pathID, true, false, true, func(item *api.Item) bool {
		if strings.EqualFold(item.Name, leaf) {
			pathIDOut = item.GetID()
			return true
		}
		return false
	})
	return pathIDOut, found, err
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (newID string, err error) {
	fs.Logf(nil, "CreateDir: Erstelle Verzeichnis '%s' in Parent-ID '%s'", leaf, pathID)
	values := url.Values{}
	values.Set("name", f.opt.Enc.FromStandardName(leaf))
	values.Set("parentId", pathID)

	type RequestCreateDir struct {
		Name     string `json:"name"`
		ParentID *int64 `json:"parentId"`
	}

	// Beim Erstellen des Requests:
	iPathId, _ := strconv.ParseInt(pathID, 10, 64)
	var parentID *int64
	if pathID != "" {
		parentID = &iPathId
	}

	requestCreateDir := RequestCreateDir{
		Name:     f.opt.Enc.FromStandardName(leaf),
		ParentID: parentID,
	}
	type ResultCreateDir struct {
		Folder struct {
			// Type        string    `json:"type,omitempty"`
			// Name        string    `json:"name,omitempty"`
			// FileName    string    `json:"file_name,omitempty"`
			// ParentID    int       `json:"parent_id,omitempty"`
			// OwnerID     int       `json:"owner_id,omitempty"`
			// WorkspaceID int       `json:"workspace_id,omitempty"`
			// UpdatedAt   time.Time `json:"updated_at,omitempty"`
			// CreatedAt   time.Time `json:"created_at,omitempty"`
			ID   int    `json:"id,omitempty"`
			Path string `json:"path,omitempty"`
		} `json:"folder,omitempty"`
		Status string `json:"status,omitempty"`
	}

	result, err := CallJSONPost[ResultCreateDir, RequestCreateDir](f, ctx, "/folders", &requestCreateDir)

	if err != nil {
		// fmt.Printf("...Error %v\n", err)
		return "", err
	}
	// fmt.Printf("...Id %q\n", *info.Id)
	return strconv.Itoa(result.Folder.ID), nil
}

// Return an Object from a path
//
// If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *api.Item) (fs.Object, error) {
	fs.Logf(nil, "newObjectWithInfo: Erstelle neues Objekt für Remote-Pfad '%s'", remote)
	o := &Object{
		fs:     f,
		remote: remote,
	}
	var err error
	if info != nil {
		// Set info
		err = o.setMetaData(info)
	} else {
		err = o.readMetaData(ctx) // reads info and meta, returning an error
	}
	if err != nil {
		return nil, err
	}
	return o, nil
}

// setMetaData sets the metadata from info
func (o *Object) setMetaData(info *api.Item) (err error) {
	fs.Logf(nil, "setMetaData: Setze Metadaten für Objekt '%v'", o)
	if info.Type == api.ItemTypeFolder {
		return fs.ErrorIsDir
	}
	if info.Type == api.ItemTypeFolder {
		return fmt.Errorf("%q is %q: %w", o.remote, info.Type, fs.ErrorNotAFile)
	}
	o.hasMetaData = true
	o.size = int64(info.FileSize)
	// o.sha1 = info.SHA1
	o.modTime = info.ModTime()
	o.id = info.GetID()
	return nil
}

// // readMetaDataForPath reads the metadata from the path
// func (f *Fs) readMetaDataForPath(ctx context.Context, path string) (info *api.Item, err error) {
// 	// defer log.Trace(f, "path=%q", path)("info=%+v, err=%v", &info, &err)
// 	leaf, directoryID, err := f.dirCache.FindPath(ctx, path, false)
// 	if err != nil {
// 		if err == fs.ErrorDirNotFound {
// 			return nil, fs.ErrorObjectNotFound
// 		}
// 		return nil, err
// 	}

// 	// Use preupload to find the ID
// 	itemMini, err := f.preUploadCheck(ctx, leaf, directoryID, -1)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if itemMini == nil {
// 		return nil, fs.ErrorObjectNotFound
// 	}

// 	// Now we have the ID we can look up the object proper
// 	opts := rest.Opts{
// 		Method:     "GET",
// 		Path:       "/files/" + itemMini.ID,
// 		Parameters: fieldsValue(),
// 	}
// 	var item api.Item
// 	err = f.pacer.Call(func() (bool, error) {
// 		resp, err := f.srv.CallJSON(ctx, &opts, nil, &item)
// 		return shouldRetry(ctx, resp, err)
// 	})
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &item, nil
// }

// // readMetaData gets the metadata if it hasn't already been fetched
// //
// // it also sets the info
// func (o *Object) readMetaData(ctx context.Context) (err error) {
// 	if o.hasMetaData {
// 		return nil
// 	}
// 	info, err := o.fs.readMetaDataForPath(ctx, o.remote)
// 	if err != nil {
// 		if apiErr, ok := err.(*api.Error); ok {
// 			if apiErr.Code == "not_found" || apiErr.Code == "trashed" {
// 				return fs.ErrorObjectNotFound
// 			}
// 		}
// 		return err
// 	}
// 	return o.setMetaData(info)
// }

// Check the interfaces are satisfied
var (
	_ fs.Fs     = (*Fs)(nil)
	_ fs.Object = (*Object)(nil)
)

// type Object interface:

// SetModTime sets the metadata on the object to set the modification date
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	fs.Logf(nil, "SetModTime wurde aufgerufen")
	panic("implement me")
	return fs.ErrorNotImplemented
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	fs.Logf(nil, "Open wurde aufgerufen")
	if o.id == "" {
		return nil, errors.New("Download nicht möglich - keine ID vorhanden")
	}

	fs.FixRangeOption(options, o.size)

	var resp *http.Response
	opts := rest.Opts{
		Method:  "GET",
		Path:    "/file-entries/download/" + o.id,
		Options: options,
	}

	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srv.Call(ctx, &opts)

		if err != nil {
			return shouldRetry(ctx, resp, err)
		}

		// Überprüfen Sie, ob es sich um eine Weiterleitung handelt
		if resp.StatusCode == http.StatusFound {
			redirectURL := resp.Header.Get("Location")
			if redirectURL == "" {
				return false, errors.New("Weiterleitungs-URL nicht gefunden")
			}

			// Folgen Sie der Weiterleitung
			redirectResp, redirectErr := http.Get(redirectURL)
			if redirectErr != nil {
				return shouldRetry(ctx, redirectResp, redirectErr)
			}

			// Ersetzen Sie die ursprüngliche Antwort durch die Weiterleitung
			resp = redirectResp
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

// Update the object with the contents of the io.Reader, modTime and size
//
// If existing is set then it updates the object rather than creating a new one.
//
// The new object may have been created if an error is returned.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	fs.Logf(nil, "Update wurde aufgerufen")
	// if o.fs.tokenRenewer != nil {
	// 	o.fs.tokenRenewer.Start()
	// 	defer o.fs.tokenRenewer.Stop()
	// }

	size := src.Size()

	if size < 0 {
		return fs.ErrorNotSupported
	}

	modTime := src.ModTime(ctx)
	remote := o.Remote()

	// Create the directory for the object if it doesn't exist
	leaf, directoryID, err := o.fs.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return err
	}

	// Upload with simple or multipart
	// if size <= int64(o.fs.opt.UploadCutoff) {
	err = o.upload(ctx, in, leaf, directoryID, size, modTime, options...)
	// } else {
	// 	err = o.uploadMultipart(ctx, in, leaf, directoryID, size, modTime, options...)
	// }
	return err
}

func getExtensionAndMime(filename string) (extension, mimeType string) {
	fs.Logf(nil, "getExtensionAndMime: Ermittle Extension und MIME-Typ für '%s'", filename)
	ext := path.Ext(filename)
	if ext == "" {
		return "bin", "application/octet-stream"
	}

	// Dateierweiterung ohne Punkt
	extension = strings.TrimPrefix(ext, ".")

	// MIME-Typ ermitteln
	mimeType = mime.TypeByExtension(ext)
	if mimeType == "" {
		// Falls kein MIME-Typ gefunden wurde, Standard zurückgeben
		mimeType = "application/octet-stream"
	}

	return extension, mimeType
}

// upload does a single non-multipart upload
//
// This is recommended for less than 50 MiB of content
// func (o *Object) upload(ctx context.Context, in io.Reader, leaf, directoryID string, size int64, modTime time.Time, options ...fs.OpenOption) (err error) {
// 	fs.Logf(nil, "upload: Lade Datei '%s' in Verzeichnis '%s' hoch (Größe: %d Bytes)", leaf, directoryID, size)
// 	// Response-Struktur für den Upload
// 	type UploadResponse struct {
// 		Status    string `json:"status"`
// 		FileEntry struct {
// 			ID        int       `json:"id"`
// 			Name      string    `json:"name"`
// 			FileName  string    `json:"file_name"`
// 			FileSize  int64     `json:"file_size"`
// 			ParentID  int       `json:"parent_id"`
// 			MimeType  string    `json:"mime"`
// 			CreatedAt time.Time `json:"created_at"`
// 		} `json:"fileEntry"`
// 	}

// 	// Erstelle einen multipart Writer
// 	var b bytes.Buffer
// 	w := multipart.NewWriter(&b)

// 	// Füge die Datei hinzu
// 	fw, err := w.CreateFormFile("file", leaf)
// 	if err != nil {
// 		return fmt.Errorf("failed to create form file: %w", err)
// 	}

// 	// Kopiere den Inhalt in den multipart Writer
// 	if _, err := io.Copy(fw, in); err != nil {
// 		return fmt.Errorf("failed to copy file: %w", err)
// 	}

// 	// Füge parentId hinzu, wenn vorhanden
// 	if directoryID != "" {
// 		if err := w.WriteField("parentId", directoryID); err != nil {
// 			return fmt.Errorf("failed to add parentId: %w", err)
// 		}
// 	}

// 	// Schließe den multipart Writer
// 	if err := w.Close(); err != nil {
// 		return fmt.Errorf("failed to close multipart writer: %w", err)
// 	}

// 	// Erstelle die Request-Optionen
// 	opts := rest.Opts{
// 		Method: "POST",
// 		Path:   "/uploads",
// 		Body:   &b,
// 		ExtraHeaders: map[string]string{
// 			"Content-Type": w.FormDataContentType(),
// 		},
// 	}

// 	// Führe den Request aus
// 	var result UploadResponse
// 	err = o.fs.pacer.Call(func() (bool, error) {
// 		resp, err := o.fs.srv.CallJSON(ctx, &opts, nil, &result)
// 		return shouldRetry(ctx, resp, err)
// 	})

// 	if err != nil {
// 		return fmt.Errorf("failed to upload file: %w", err)
// 	}

// 	// Überprüfe den Status
// 	if result.Status != "success" {
// 		return fmt.Errorf("upload failed: %s", result.Status)
// 	}

// 	// Aktualisiere das Object mit den erhaltenen Daten
// 	o.id = strconv.Itoa(result.FileEntry.ID)
// 	o.size = result.FileEntry.FileSize
// 	o.modTime = result.FileEntry.CreatedAt
// 	o.mimeType = result.FileEntry.MimeType
// 	o.hasMetaData = true

// 	return nil
// }

func (o *Object) upload(ctx context.Context, in io.Reader, leaf, directoryID string, size int64, modTime time.Time, options ...fs.OpenOption) (err error) {
	directoryIDInt, _ := strconv.Atoi(directoryID)

	// Anfordern der vorzeichneten URL
	type ResultPresign struct {
		URL    string `json:"url"`
		Key    string `json:"key"`
		ACL    string `json:"acl"`
		Status string `json:"status"`
	}

	type RequestPresign struct {
		Filename     string `json:"filename"`
		Mime         string `json:"mime"`
		Disk         string `json:"disk"`
		Size         int64  `json:"size"`
		Extension    string `json:"extension"`
		WorkspaceID  int    `json:"workspaceId"`
		ParentID     int    `json:"parentId"`
		RelativePath string `json:"relativePath"`
	}

	ext, mime := getExtensionAndMime(leaf)

	requestPresign := RequestPresign{
		Filename:     leaf,
		Mime:         mime,
		Disk:         "uploads",
		Size:         size,
		Extension:    ext,
		WorkspaceID:  0,
		ParentID:     directoryIDInt,
		RelativePath: "",
	}

	resultPresign, err := CallJSONPost[ResultPresign, RequestPresign](o.fs, ctx, "/s3/simple/presign", &requestPresign)

	if err != nil {
		return fmt.Errorf("fehler beim Anfordern der vorzeichneten URL: %w", err)
	}

	if resultPresign.Status != "success" {
		return fmt.Errorf("fehler beim Anfordern der vorzeichneten URL: Status ist nicht 'success'")
	}

	// // OPTIONS-Request
	// optionsReq, err := http.NewRequestWithContext(ctx, "OPTIONS", resultPresign.URL, nil)
	// if err != nil {
	// 	return fmt.Errorf("fehler beim Erstellen des OPTIONS-Requests: %w", err)
	// }

	// optionsResp, err := http.DefaultClient.Do(optionsReq)
	// if err != nil {
	// 	return fmt.Errorf("fehler beim Ausführen des OPTIONS-Requests: %w", err)
	// }
	// optionsResp.Body.Close()

	// PUT-Request
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, resultPresign.URL, in)
	if err != nil {
		return fmt.Errorf("fehler beim Erstellen des PUT-Requests: %w", err)
	}

	// // Erstellen Sie einen einmaligen Client nur für diesen Request
	// client := &http.Client{
	// 	Transport: &http.Transport{
	// 		Proxy: func(_ *http.Request) (*url.URL, error) {
	// 			return url.Parse("http://localhost:8888")
	// 		},
	// 		TLSClientConfig: &tls.Config{
	// 			InsecureSkipVerify: true,
	// 		},
	// 	},
	// }

	// Setzen Sie hier die notwendigen Header
	putReq.Header.Set("Content-Type", "application/octet-stream")
	putReq.Header.Set("x-amz-acl", resultPresign.ACL)

	// putResp, err := client.Do(putReq)
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		return fmt.Errorf("fehler beim Hochladen der Datei: %w", err)
	}
	defer putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(putResp.Body)
		return fmt.Errorf("fehler beim Hochladen der Datei: HTTP %d: %s", putResp.StatusCode, string(body))
		// } else {
		// 	body, _ := io.ReadAll(putResp.Body)
		// 	fs.Log(nil, fmt.Sprintf("Datei hochgeladen: HTTP %v: %s", putResp.StatusCode, string(body)))
	}

	type RequestEntries struct {
		WorkspaceID     int         `json:"workspaceId"`
		ParentID        interface{} `json:"parentId"`
		RelativePath    string      `json:"relativePath"`
		Disk            string      `json:"disk"`
		ClientMime      string      `json:"clientMime"`
		ClientName      string      `json:"clientName"`
		Filename        string      `json:"filename"`
		Size            int64       `json:"size"`
		ClientExtension string      `json:"clientExtension"`
	}
	requestEntries := RequestEntries{
		WorkspaceID: 0,
		ParentID: func() interface{} {
			if directoryIDInt == 0 {
				return ""
			}
			return directoryIDInt
		}(),
		RelativePath:    "",
		Disk:            "uploads",
		ClientMime:      "application/octet-stream",
		ClientName:      leaf,
		Filename:        path.Base(resultPresign.Key),
		Size:            size,
		ClientExtension: "bin",
	}

	resultEntries, err := CallJSONPost[api.Item, RequestEntries](o.fs, ctx, "/s3/entries", &requestEntries)

	if err != nil {
		return fmt.Errorf("fehler beim Anfordern der Datei-Daten URL: %w", err)
	}

	// Setzen der Metadaten des Objekts
	err = o.setMetaData(resultEntries)
	if err != nil {
		return fmt.Errorf("fehler beim Setzen der Metadaten: %w", err)
	}

	return nil
}

// Removes this object
func (o *Object) Remove(ctx context.Context) error {
	fs.Logf(nil, "Remove: Lösche Objekt '%s' (ID: %s)", o.remote, o.id)
	type RequestDelete struct {
		EntryIds      []int `json:"entryIds"`
		DeleteForever bool  `json:"deleteForever"`
	}

	type ResultDelete struct {
		Status string `json:"status,omitempty"`
	}

	entryId, _ := strconv.Atoi(o.id)
	request := RequestDelete{
		EntryIds:      []int{entryId},
		DeleteForever: true,
	}

	result, err := CallJSONPost[ResultDelete, RequestDelete](o.fs, ctx, "/file-entries/delete", &request)

	if err != nil {
		return err
	}
	if result.Status != "success" {
		return errors.New(result.Status)
	}
	return nil
}

// type ObjectInfo interface:

// Fs returns read only access to the Fs that this object is part of
func (o *Object) Fs() fs.Info {
	fs.Logf(nil, "Fs wurde aufgerufen")
	return o.fs
}

// Hash returns the selected checksum of the file
// If no checksum is available it returns ""
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	fs.Logf(nil, "Hash wurde aufgerufen")
	return "", hash.ErrUnsupported
}

// Storable says whether this object can be stored
func (o *Object) Storable() bool {
	fs.Logf(nil, "Storable wurde aufgerufen")
	return true
}

// type DirEntry interface:
// String returns a description of the Object
func (o *Object) String() string {
	fs.Logf(nil, "String: Erstelle String-Repräsentation für '%s'", o.remote)
	if o.remote == "" {
		return ""
	}
	err := o.readMetaData(context.Background())
	if err != nil {
		return err.Error()
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	fs.Logf(nil, "Remote: Hole Remote-Pfad für '%s'", o.remote)
	return o.remote
}

// readMetaDataForPath reads the metadata from the path
func (f *Fs) readMetaDataForPath(ctx context.Context, path string) (info *api.Item, err error) {
	fs.Logf(nil, "readMetaDataForPath: Lese Metadaten für Pfad '%s'", path)
	// defer fs.Trace(f, "path=%q", path)("info=%+v, err=%v", &info, &err)
	leaf, directoryID, err := f.dirCache.FindPath(ctx, path, false)
	if err != nil {
		if err == fs.ErrorDirNotFound {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}

	found, err := f.listAll(ctx, directoryID, false, true, false, func(item *api.Item) bool {
		if item.Name == leaf {
			info = item
			return true
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fs.ErrorObjectNotFound
	}
	return info, nil
}

// readMetaData gets the metadata if it hasn't already been fetched
//
// it also sets the info
func (o *Object) readMetaData(ctx context.Context) (err error) {
	fs.Logf(nil, "readMetaData: Lese Metadaten für Objekt '%s'", o.remote)
	if o.hasMetaData {
		return nil
	}
	info, err := o.fs.readMetaDataForPath(ctx, o.remote)
	if err != nil {
		if err.Error() == "object not found" {
			return fs.ErrorObjectNotFound
		}
		return err
	}
	return o.setMetaData(info)
}

// ModTime returns the modification date of the file
// It should return a best guess if one isn't available
func (o *Object) ModTime(ctx context.Context) time.Time {
	fs.Logf(nil, "ModTime: Hole Modifikationszeit für '%s'", o.remote)
	err := o.readMetaData(ctx)
	if err != nil {
		fs.Logf(o, "Failed to read metadata: %v", err)
		return time.Now()
	}
	return o.modTime
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	fs.Logf(nil, "Size: Hole Größe für '%s'", o.remote)
	err := o.readMetaData(context.TODO())
	if err != nil {
		fs.Logf(o, "Failed to read metadata: %v", err)
		return -1
	}
	return o.size
}