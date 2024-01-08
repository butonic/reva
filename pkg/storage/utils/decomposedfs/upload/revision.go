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
	"time"

	"github.com/cs3org/reva/v2/internal/grpc/services/storageprovider"
	"github.com/cs3org/reva/v2/pkg/storage/utils/decomposedfs/lookup"
	"github.com/cs3org/reva/v2/pkg/storage/utils/decomposedfs/metadata/prefixes"
	"github.com/cs3org/reva/v2/pkg/storage/utils/decomposedfs/node"
	"github.com/pkg/errors"
	"github.com/rogpeppe/go-internal/lockedfile"
)

func CreateRevisionNode(ctx context.Context, lu *lookup.Lookup, revisionNode *node.Node) (*lockedfile.File, error) {
	revisionPath := revisionNode.InternalPath()

	// write lock existing node before reading any metadata
	f, err := lockedfile.OpenFile(lu.MetadataBackend().LockfilePath(revisionPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	// FIXME if this is removed listing revisions breaks because it globs the dir but then filters all metadata files
	// we also need to touch the versions node here to list revisions
	h, err := os.OpenFile(revisionPath, os.O_CREATE /*|os.O_EXCL*/, 0600) // we have to allow overwriting revisions to be oc10 compatible
	if err != nil {
		return f, err
	}
	h.Close()
	return f, nil
}

func SetNodeToUpload(ctx context.Context, lu *lookup.Lookup, n *node.Node, rm RevisionMetadata) (int64, error) {

	nodePath := n.InternalPath()
	// lock existing node metadata
	nh, err := lockedfile.OpenFile(lu.MetadataBackend().LockfilePath(nodePath), os.O_RDWR, 0600)
	if err != nil {
		return 0, err
	}
	defer nh.Close()
	// read nodes

	n, err = node.ReadNode(ctx, lu, n.SpaceID, n.ID, false, n.SpaceRoot, true)
	if err != nil {
		return 0, err
	}

	sizeDiff := rm.BlobSize - n.Blobsize

	n.BlobID = rm.BlobID
	n.Blobsize = rm.BlobSize

	if rm.MTime.IsZero() {
		rm.MTime = time.Now().UTC()
	}

	// update node
	err = WriteRevisionMetadataToNode(ctx, n, rm)
	if err != nil {
		return 0, errors.Wrap(err, "Decomposedfs: could not write metadata")
	}

	return sizeDiff, nil
}

// RevisionMetadata is all the metadata that will be persisted in revisions
// MTime in tracked as part of the revision name and not in the content so restoring revisions can update the revision by renaming it
type RevisionMetadata struct {
	MTime           time.Time
	BlobID          string
	BlobSize        int64
	ChecksumSHA1    []byte
	ChecksumMD5     []byte
	ChecksumADLER32 []byte
}

func WriteRevisionMetadataToNode(ctx context.Context, n *node.Node, revisionMetadata RevisionMetadata) error {
	attrs := node.Attributes{}
	attrs.SetString(prefixes.BlobIDAttr, revisionMetadata.BlobID)
	attrs.SetInt64(prefixes.BlobsizeAttr, revisionMetadata.BlobSize)
	attrs[prefixes.ChecksumPrefix+storageprovider.XSSHA1] = revisionMetadata.ChecksumSHA1
	attrs[prefixes.ChecksumPrefix+storageprovider.XSMD5] = revisionMetadata.ChecksumMD5
	attrs[prefixes.ChecksumPrefix+storageprovider.XSAdler32] = revisionMetadata.ChecksumADLER32

	return n.SetXattrsWithContext(ctx, attrs, false)
}
