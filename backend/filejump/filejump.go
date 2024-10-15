package filejump

import (
	"context"
	"fmt"
	"io"
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
	AccessToken string               `config:"access_token"`
	Enc         encoder.MultiEncoder `config:"encoding"`
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
	opts := rest.Opts{
		Method: "GET",
		Path:   "/drive/file-entries",
	}

	values := url.Values{}
	values.Set("folderId", dirID)
	// values.Set("parentIds", dirID)
	values.Set("perPage", "1000")
	opts.Parameters = values
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
			opts.Parameters.Set("page", strconv.FormatUint(uint64(*page), 10))
		}

		var result api.FileEntries
		var resp *http.Response
		err = f.pacer.Call(func() (bool, error) {
			resp, err = f.srv.CallJSON(ctx, &opts, nil, &result)
			return shouldRetry(ctx, resp, err)
		})
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
	fs.Logf(nil, "CreateDir wurde aufgerufen")
	return "", fs.ErrorNotImplemented
}

// Return an Object from a path
//
// If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *api.Item) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	var err error
	if info != nil {
		// Set info
		err = o.setMetaData(info)
		// } else {
		// 	err = o.readMetaData(ctx) // reads info and meta, returning an error
	}
	if err != nil {
		return nil, err
	}
	return o, nil
}

// setMetaData sets the metadata from info
func (o *Object) setMetaData(info *api.Item) (err error) {
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
