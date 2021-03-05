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

package posix

import (
	"context"
	"os"
	"path/filepath"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/pkg/errtypes"
	"github.com/pkg/errors"
)

// Tree manages a hierarchical tree
type Tree struct {
	lu *Lookup
}

// NewTree creates a new Tree instance
func NewTree(lu *Lookup) (TreePersistence, error) {
	return &Tree{
		lu: lu,
	}, nil
}

// GetMD returns the metadata of a node in the tree
func (t *Tree) GetMD(ctx context.Context, node *Node) (os.FileInfo, error) {
	md, err := os.Stat(node.InternalPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errtypes.NotFound(node.ID())
		}
		return nil, errors.Wrap(err, "tree: error stating "+node.ID())
	}

	return md, nil
}

// GetPathByID returns the fn pointed by the file id, without the internal namespace
func (t *Tree) GetPathByID(ctx context.Context, id *provider.ResourceId) (relativeExternalPath string, err error) {
	var node *Node
	node, err = t.lu.NodeFromID(ctx, id)
	if err != nil {
		return
	}

	relativeExternalPath, err = t.lu.Path(ctx, node)
	return
}

func createNode(n *Node, owner *userpb.UserId) (err error) {
	// create a directory node
	if err = os.MkdirAll(n.InternalPath(), 0700); err != nil {
		return errors.Wrap(err, "posixfs: error creating node")
	}
	n.owner = owner
	return n.writeMetadata()
}

// CreateDir creates a new directory entry in the tree
func (t *Tree) CreateDir(ctx context.Context, node *Node) (err error) {

	if node.Exists || node.ID() != "" {
		return errtypes.AlreadyExists(node.ID()) // path?
	}

	// who will become the owner? the owner of the parent node, not the current user
	var p *Node
	p, err = node.Parent()
	if err != nil {
		return
	}
	var owner *userpb.UserId
	owner, err = p.Owner()
	if err != nil {
		return
	}

	return createNode(node, owner)
}

// Move replaces the target with the source
func (t *Tree) Move(ctx context.Context, oldNode *Node, newNode *Node) (err error) {

	// rename child
	err = os.Rename(oldNode.InternalPath(), newNode.InternalPath())
	if err != nil {
		return errors.Wrap(err, "posixfs: could not rename node")
	}

	return nil
}

// ListFolder lists the content of a folder node
func (t *Tree) ListFolder(ctx context.Context, node *Node) ([]*Node, error) {
	dir := node.InternalPath()
	f, err := os.Open(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errtypes.NotFound(dir)
		}
		return nil, errors.Wrap(err, "tree: error listing "+dir)
	}
	defer f.Close()

	fis, err := f.Readdir(0)
	if err != nil {
		return nil, err
	}
	nodes := []*Node{}
	for i := range fis {
		n := &Node{
			lu:   t.lu,
			Dir:  filepath.Join(node.Dir, node.Name),
			Name: fis[i].Name(),
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// Delete deletes a node in the tree
func (t *Tree) Delete(ctx context.Context, n *Node) (err error) {
	return os.Remove(n.InternalPath())
}

// Propagate propagates changes to the root of the tree
func (t *Tree) Propagate(ctx context.Context, n *Node) (err error) {
	return nil
}
