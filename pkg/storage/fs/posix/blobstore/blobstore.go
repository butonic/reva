// Copyright 2018-2021 CERN
// Copyright 2025 OpenCloud GmbH <mail@opencloud.eu>
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

package blobstore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/pkg/xattr"

	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/disk"
	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/metadata/prefixes"
	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/node"
)

const (
	TMPDir     = ".oc-tmp"
	fsyncEvery = 16 << 20 // 16 MiB
)

// Blobstore provides an interface to an filesystem based blobstore
type Blobstore struct {
	root string

	canUseRenameForUpload bool
}

// New returns a new Blobstore
func New(root string) (*Blobstore, error) {
	return &Blobstore{
		root:                  root,
		canUseRenameForUpload: true, // let's assume the upload area is on the same device as the blobstore root by default
	}, nil
}

// Upload is responsible for transferring data from a source file (upload) to its final location.
// The node's oc. xattrs and mtime are set on the blob before it atomically replaces the node. If
// the source is on the node's filesystem, it is prepared in place and moved onto the node with a
// single rename. Otherwise it is copied into the space's tmp dir first.
func (bs *Blobstore) Upload(n *node.Node, source, copyTarget string) error {
	// read everything the blob needs before touching a file, so a failure leaves the source alone
	attrs, mtime, err := blobAttributes(n)
	if err != nil {
		return err
	}

	if bs.canUseRenameForUpload {
		same, err := sameDevice(source, filepath.Dir(n.InternalPath()))
		if err != nil {
			return err
		}
		if same {
			err := moveIntoPlace(n, source, attrs, mtime, copyTarget)
			if !errors.Is(err, syscall.EXDEV) {
				return err
			}
			// same device but a different mount, e.g. a bind mount: copy from now on
			bs.canUseRenameForUpload = false
		}
	}

	tempName := filepath.Join(n.SpaceRoot.InternalPath(), TMPDir, filepath.Base(source))

	// there is no guarantee that the space root TMPDir exists at this point, so we create the directory if needed
	if err := os.MkdirAll(filepath.Dir(tempName), 0700); err != nil {
		return err
	}

	sourceFile, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("failed to open source file '%s': %v", source, err)
	}
	defer func() {
		_ = sourceFile.Close()
	}()

	tempFile, err := os.OpenFile(tempName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("unable to create temp file '%s': %v", tempName, err)
	}

	if err := copyWithPeriodicSync(tempFile, sourceFile); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("failed to copy source file '%s' to temp file '%s' - %v", source, tempName, err)
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file '%s' - %v", tempName, err)
	}

	return moveIntoPlace(n, tempName, attrs, mtime, copyTarget)
}

// blobAttributes returns the node's oc. xattrs and its mtime, which a blob must carry when it
// replaces the node: on posix the node's metadata lives on the file itself.
func blobAttributes(n *node.Node) (map[string][]byte, time.Time, error) {
	nodeAttributes, err := n.Xattrs(context.Background())
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to get xattrs for node '%s': %v", n.InternalPath(), err)
	}

	attrs := map[string][]byte{}
	var mtime *time.Time
	for k, v := range nodeAttributes {
		if !strings.HasPrefix(k, prefixes.OcPrefix) {
			continue
		}
		attrs[k] = v

		if k == prefixes.MTimeAttr {
			tv, err := time.Parse(time.RFC3339Nano, string(v))
			if err == nil {
				mtime = &tv
			}
		}
	}

	// the extended attributes should always contain a mtime, but in case they don't, we fetch it from the node
	if mtime == nil {
		nodeMtime, err := n.GetMTime(context.Background())
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("failed to get mtime for node '%s' - %v", n.InternalPath(), err)
		}
		mtime = &nodeMtime
	}
	return attrs, *mtime, nil
}

// sameDevice reports whether both paths are on the same device
func sameDevice(a, b string) (bool, error) {
	ai, err := os.Stat(a)
	if err != nil {
		return false, fmt.Errorf("failed to stat '%s': %v", a, err)
	}
	bi, err := os.Stat(b)
	if err != nil {
		return false, fmt.Errorf("failed to stat '%s': %v", b, err)
	}
	as, aok := ai.Sys().(*syscall.Stat_t)
	bs, bok := bi.Sys().(*syscall.Stat_t)
	return aok && bok && as.Dev == bs.Dev, nil
}

// moveIntoPlace sets the node's attributes on the blob at blobPath and renames it onto the node.
// The rename error is wrapped, so the caller can detect EXDEV.
func moveIntoPlace(n *node.Node, blobPath string, attrs map[string][]byte, mtime time.Time, copyTarget string) error {
	for k, v := range attrs {
		if err := xattr.Set(blobPath, k, v); err != nil {
			return fmt.Errorf("failed to set xattr '%s' on '%s' - %v", k, blobPath, err)
		}
	}

	// etags rely on the id and the mtime, so we need to ensure the mtime is set correctly
	if err := os.Chtimes(blobPath, mtime, mtime); err != nil {
		return fmt.Errorf("failed to set mtime on '%s' - %v", blobPath, err)
	}

	// keep a handle on the blob before it moves to its final location. the handle stays valid
	// across the rename, so the copy below needs no second lookup of the final path
	var blobFile *os.File
	if copyTarget != "" {
		var err error
		blobFile, err = os.Open(blobPath)
		if err != nil {
			return errors.Wrapf(err, "could not open '%s' for reading", blobPath)
		}
		defer func() {
			_ = blobFile.Close()
		}()
	}

	// atomically move the file to its final location,
	// on Windows systems (unsupported oc os) os.Rename is not atomic
	if err := os.Rename(blobPath, n.InternalPath()); err != nil {
		return fmt.Errorf("failed to move '%s' to node '%s': %w", blobPath, n.InternalPath(), err)
	}

	// upload successfully, now handle the copy target if set
	if copyTarget == "" {
		return nil
	}

	// also "upload" the file to a local path, e.g., for keeping the "current" version of the file
	if err := os.MkdirAll(filepath.Dir(copyTarget), 0700); err != nil {
		return err
	}

	copyFile, err := os.OpenFile(copyTarget, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return errors.Wrapf(err, "could not open copy target '%s' for writing", copyTarget)
	}
	defer func() {
		_ = copyFile.Close()
	}()

	if err := copyWithPeriodicSync(copyFile, blobFile); err != nil {
		return errors.Wrapf(err, "could not write blob copy of '%s' to '%s'", n.InternalPath(), copyTarget)
	}

	return nil
}

// UploadFromReader stages data on the space filesystem before atomically replacing the node.
func (bs *Blobstore) UploadFromReader(n *node.Node, r io.Reader, size int64) error {
	if n.SpaceRoot == nil {
		return errors.New("node has no space root")
	}
	f, err := os.CreateTemp(n.SpaceRoot.InternalPath(), ".upload-*")
	if err != nil {
		return errors.Wrap(err, "could not create temporary upload file")
	}
	tempPath := f.Name()
	defer func() { _ = os.Remove(tempPath) }()

	written, err := io.Copy(f, r)
	if err != nil {
		_ = f.Close()
		return errors.Wrap(err, "could not write temporary upload file")
	}
	if written != size {
		_ = f.Close()
		return fmt.Errorf("unexpected upload length: expected %d bytes, got %d", size, written)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return errors.Wrap(err, "could not sync temporary upload file")
	}
	if err := f.Close(); err != nil {
		return errors.Wrap(err, "could not close temporary upload file")
	}

	return bs.Upload(n, tempPath, "")
}

// Download retrieves a blob from the blobstore for reading
func (bs *Blobstore) Download(node *node.Node) (io.ReadCloser, error) {
	file, err := os.Open(node.InternalPath())
	if err != nil {
		return nil, errors.Wrapf(err, "could not read blob '%s'", node.InternalPath())
	}
	return file, nil
}

// Delete deletes a blob from the blobstore
func (bs *Blobstore) Delete(node *node.Node) error {
	return nil
}

func copyWithPeriodicSync(dst, src *os.File) error {
	for {
		n, err := io.CopyN(dst, src, fsyncEvery)
		if n > 0 {
			if serr := disk.Fdatasync(dst); serr != nil {
				return serr
			}
		}
		if err == io.EOF {
			return dst.Sync()
		}
		if err != nil {
			return err
		}
	}
}
