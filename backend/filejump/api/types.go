package api

import (
	"strconv"
	"time"
)

// Types of things in Item/ItemMini
const (
	ItemTypeFolder = "folder"
	ItemTypeImage  = "image"
	ItemTypeText   = "text"
	ItemTypeAudio  = "audio"
	ItemTypeVideo  = "video"
	ItemTypePdf    = "pdf"
	// ItemStatusActive  = "active"
	// ItemStatusDeleted = "deleted"
)

type FileEntries struct {
	CurrentPage uint   `json:"current_page,omitempty"`
	Data        []Item `json:"data,omitempty"`
	From        uint   `json:"from,omitempty"`
	NextPage    *uint  `json:"next_page,omitempty"`
	PerPage     uint   `json:"per_page,omitempty"`
	PrevPage    *uint  `json:"prev_page,omitempty"`
	To          uint   `json:"to,omitempty"`
	Folder      Folder `json:"folder,omitempty"`
}

type Item struct {
	ID          int       `json:"id,omitempty"`
	Name        string    `json:"name,omitempty"`
	Description any       `json:"description,omitempty"`
	FileName    string    `json:"file_name,omitempty"`
	Mime        any       `json:"mime,omitempty"`
	FileSize    int       `json:"file_size,omitempty"`
	UserID      any       `json:"user_id,omitempty"`
	ParentID    any       `json:"parent_id,omitempty"`
	CreatedAt   time.Time `json:"created_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
	DeletedAt   any       `json:"deleted_at,omitempty"`
	Path        string    `json:"path,omitempty"`
	DiskPrefix  any       `json:"disk_prefix,omitempty"`
	Type        string    `json:"type,omitempty"`
	Extension   any       `json:"extension,omitempty"`
	Public      bool      `json:"public,omitempty"`
	Thumbnail   bool      `json:"thumbnail,omitempty"`
	WorkspaceID int       `json:"workspace_id,omitempty"`
	OwnerID     int       `json:"owner_id,omitempty"`
	Hash        string    `json:"hash,omitempty"`
	URL         any       `json:"url,omitempty"`
	Tags        []any     `json:"tags,omitempty"`
}

func (i *Item) GetID() (id string) {
	return strconv.Itoa(i.ID)
}

// ModTime returns the modification time of the item
func (i *Item) ModTime() (t time.Time) {
	t = time.Time(i.UpdatedAt)
	if t.IsZero() {
		t = time.Time(i.CreatedAt)
	}
	return t
}

type Folder struct {
	Type        string `json:"type,omitempty"`
	ID          int    `json:"id,omitempty"`
	Hash        string `json:"hash,omitempty"`
	Path        string `json:"path,omitempty"`
	WorkspaceID int    `json:"workspace_id,omitempty"`
	Name        string `json:"name,omitempty"`
}
