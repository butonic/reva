// Copyright 2018-2022 CERN
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

package upload

import (
	"context"
	"os"
	"path/filepath"
	"time"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v2/pkg/appctx"
	ctxpkg "github.com/cs3org/reva/v2/pkg/ctx"
	"github.com/cs3org/reva/v2/pkg/errtypes"
	"github.com/cs3org/reva/v2/pkg/storage/utils/decomposedfs/lookup"
	"github.com/cs3org/reva/v2/pkg/storage/utils/decomposedfs/metadata/prefixes"
	"github.com/cs3org/reva/v2/pkg/storage/utils/decomposedfs/node"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rogpeppe/go-internal/lockedfile"
	"github.com/shamaton/msgpack/v2"
)

type Session struct {
	ID                  string
	Filename            string
	SpaceRoot           string
	SpaceOwnerOrManager string
	ProviderID          string
	MTime               string
	NodeID              string
	NodeParentID        string
	ExecutantIdp        string
	ExecutantID         string
	ExecutantType       string
	ExecutantUserName   string
	LogLevel            string
	Checksum            string
	ChecksumSHA1        []byte
	ChecksumADLER32     []byte
	ChecksumMD5         []byte

	BlobID   string
	BlobSize int64

	Chunk                   string
	Dir                     string
	LockID                  string
	HeaderIfMatch           string
	HeaderIfNoneMatch       string
	HeaderIfUnmodifiedSince string
	Expires                 time.Time
}

// WriteMetadata will create a metadata file to keep track of an upload
func WriteMetadata(ctx context.Context, lu *lookup.Lookup, uploadID string, metadata Session) error {
	_, span := tracer.Start(ctx, "WriteMetadata")
	defer span.End()

	uploadPath := lu.UploadPath(uploadID)

	// create folder structure (if needed)
	if err := os.MkdirAll(filepath.Dir(uploadPath), 0700); err != nil {
		return err
	}

	var d []byte
	d, err := msgpack.Marshal(metadata)
	if err != nil {
		return err
	}

	_, subspan := tracer.Start(ctx, "os.Writefile")
	err = os.WriteFile(uploadPath, d, 0600)
	subspan.End()
	if err != nil {
		return err
	}

	return nil
}
func ReadMetadata(ctx context.Context, lu *lookup.Lookup, uploadID string) (Session, error) {
	_, span := tracer.Start(ctx, "ReadMetadata")
	defer span.End()

	uploadPath := lu.UploadPath(uploadID)

	_, subspan := tracer.Start(ctx, "os.ReadFile")
	msgBytes, err := os.ReadFile(uploadPath)
	subspan.End()
	if err != nil {
		return Session{}, err
	}

	metadata := Session{}
	if len(msgBytes) > 0 {
		err = msgpack.Unmarshal(msgBytes, &metadata)
		if err != nil {
			return Session{}, err
		}
	}
	return metadata, nil
}

// UpdateMetadata will create the target node for the Upload
// - if the node does not exist it is created and assigned an id, no blob id?
// - then always write out a revision node
// - when postprocessing finishes copy metadata to node and replace latest revision node with previous blob info. if blobid is empty delete previous revision completely?
func UpdateMetadata(ctx context.Context, lu *lookup.Lookup, uploadID string, size int64, uploadMetadata Session) (Session, *node.Node, error) {
	ctx, span := tracer.Start(ctx, "UpdateMetadata")
	defer span.End()
	log := appctx.GetLogger(ctx).With().Str("uploadID", uploadID).Logger()

	// check lock
	if uploadMetadata.LockID != "" {
		ctx = ctxpkg.ContextSetLockID(ctx, uploadMetadata.LockID)
	}

	var err error
	var n *node.Node
	var nodeHandle *lockedfile.File
	if uploadMetadata.NodeID == "" {
		// we need to check if the node exists via parentid & child name
		p, err := node.ReadNode(ctx, lu, uploadMetadata.SpaceRoot, uploadMetadata.NodeParentID, false, nil, true)
		if err != nil {
			log.Error().Err(err).Msg("could not read parent node")
			return Session{}, nil, err
		}
		if !p.Exists {
			return Session{}, nil, errtypes.PreconditionFailed("parent does not exist")
		}
		n, err = p.Child(ctx, uploadMetadata.Filename)
		if err != nil {
			log.Error().Err(err).Msg("could not read child node")
			return Session{}, nil, err
		}
		if !n.Exists {
			n.ID = uuid.New().String()
			nodeHandle, err = initNewNode(ctx, lu, uploadID, uploadMetadata.MTime, n)
			if err != nil {
				log.Error().Err(err).Msg("could not init new node")
				return Session{}, nil, err
			}
		} else {
			nodeHandle, err = openExistingNode(ctx, lu, n)
			if err != nil {
				log.Error().Err(err).Msg("could not open existing node")
				return Session{}, nil, err
			}
		}
	}

	if nodeHandle == nil {
		n, err = node.ReadNode(ctx, lu, uploadMetadata.SpaceRoot, uploadMetadata.NodeID, false, nil, true)
		if err != nil {
			log.Error().Err(err).Msg("could not read parent node")
			return Session{}, nil, err
		}
		nodeHandle, err = openExistingNode(ctx, lu, n)
		if err != nil {
			log.Error().Err(err).Msg("could not open existing node")
			return Session{}, nil, err
		}
	}
	defer func() {
		if nodeHandle == nil {
			return
		}
		if err := nodeHandle.Close(); err != nil {
			log.Error().Err(err).Str("nodeid", n.ID).Str("parentid", n.ParentID).Msg("could not close lock")
		}
	}()

	err = validateRequest(ctx, size, uploadMetadata, n)
	if err != nil {
		return Session{}, nil, err
	}

	// set processing status of node
	nodeAttrs := node.Attributes{}
	// store Blobsize in node so we can propagate a sizediff
	// do not yet update the blobid ... urgh this is fishy
	nodeAttrs.SetString(prefixes.StatusPrefix, node.ProcessingStatus+uploadID)
	err = n.SetXattrsWithContext(ctx, nodeAttrs, false)
	if err != nil {
		return Session{}, nil, errors.Wrap(err, "Decomposedfs: could not write metadata")
	}

	uploadMetadata.BlobSize = size
	// TODO we should persist all versions as writes with ranges and the blobid in the node metadata

	err = WriteMetadata(ctx, lu, uploadID, uploadMetadata)
	if err != nil {
		return Session{}, nil, errors.Wrap(err, "Decomposedfs: could not write upload metadata")
	}

	return uploadMetadata, n, nil
}

func (m Session) GetID() string {
	return m.ID
}
func (m Session) GetFilename() string {
	return m.Filename
}

// TODO use uint64? use SizeDeferred flag is in tus? cleaner then int64 and a negative value
func (m Session) GetSize() int64 {
	return m.BlobSize
}
func (m Session) GetResourceID() provider.ResourceId {
	return provider.ResourceId{
		StorageId: m.ProviderID,
		SpaceId:   m.SpaceRoot,
		OpaqueId:  m.NodeID,
	}
}
func (m Session) GetReference() provider.Reference {
	return provider.Reference{
		ResourceId: &provider.ResourceId{
			StorageId: m.ProviderID,
			SpaceId:   m.SpaceRoot,
			OpaqueId:  m.NodeID,
		},
		// Path is not used
	}
}
func (m Session) GetExecutantID() userpb.UserId {
	return userpb.UserId{
		Type:     userpb.UserType(userpb.UserType_value[m.ExecutantType]),
		Idp:      m.ExecutantIdp,
		OpaqueId: m.ExecutantID,
	}
}
func (m Session) GetSpaceOwner() userpb.UserId {
	return userpb.UserId{
		// idp and type do not seem to be consumed and the node currently only stores the user id anyway
		OpaqueId: m.SpaceOwnerOrManager,
	}

}
func (m Session) GetExpires() time.Time {
	return m.Expires
}
