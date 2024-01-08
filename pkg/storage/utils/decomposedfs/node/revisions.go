// Copyright 2018-2021 CERN
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

package node

import (
	"context"
	"strings"

	"github.com/cs3org/reva/v2/pkg/storage/utils/decomposedfs/metadata"
	"github.com/cs3org/reva/v2/pkg/storage/utils/decomposedfs/metadata/prefixes"
)

// Define keys and values used in the node metadata
const (
	RevisionIDDelimiter = ".REV."
)

// SplitRevisionKey splits revision key into nodeid and revisionTime
func SplitRevisionKey(revisionKey string) (string, string) {
	parts := strings.SplitN(revisionKey, RevisionIDDelimiter, 2)
	if len(parts) != 2 {
		return revisionKey, ""
	}
	return parts[0], parts[1]
}

// JoinRevisionKey joins nodeid and revision into revision key
func JoinRevisionKey(nodeID, revision string) string {
	return nodeID + RevisionIDDelimiter + revision
}

// RevisionNode will return a node for the revision without reading the metadata
func (n *Node) RevisionNode(ctx context.Context, revision string) *Node {
	return &Node{
		SpaceID:  n.SpaceID,
		ID:       JoinRevisionKey(n.ID, revision),
		ParentID: n.ParentID,
		Name:     n.Name,
		owner:    n.owner,
		lu:       n.lu,
		nodeType: n.nodeType,
	}
}

// ReadRevision will return a node for the revision and read the metadata
func (n *Node) ReadRevision(ctx context.Context, revision string) (*Node, error) {
	rn := n.RevisionNode(ctx, revision)
	attrs, err := rn.Xattrs(ctx)
	switch {
	case metadata.IsNotExist(err):
		return rn, nil // swallow not found, the node defaults to exists = false
	case err != nil:
		return nil, err
	}
	rn.Exists = true

	rn.BlobID = attrs.String(prefixes.BlobIDAttr)
	rn.Blobsize, err = attrs.Int64(prefixes.BlobsizeAttr)
	if err != nil {
		return nil, err
	}

	return rn, nil
}
