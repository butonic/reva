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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	types "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	"github.com/cs3org/reva/internal/grpc/services/storageprovider"
	"github.com/cs3org/reva/pkg/appctx"
	"github.com/cs3org/reva/pkg/errtypes"
	"github.com/cs3org/reva/pkg/mime"
	"github.com/cs3org/reva/pkg/storage/utils/ace"
	"github.com/cs3org/reva/pkg/user"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/pkg/xattr"
	"github.com/rs/zerolog/log"
)

const (
	_shareTypesKey = "http://owncloud.org/ns/share-types"
	_userShareType = "0"

	_favoriteKey  = "http://owncloud.org/ns/favorite"
	_checksumsKey = "http://owncloud.org/ns/checksums"
)

// Node represents a node in the tree and provides methods to get a Parent or Child instance
type Node struct {
	lu    *Lookup
	owner *userpb.UserId
	Dir   string
	// inode:id is the id that is given to the outside
	// internally the id is stored as an extended attribute
	// the inode remains the same unless the file is copied on disk, which creates a new file -> needs a new id?
	// TODO store inode:id to be able to detect a copy and create a new fileid then
	id     string
	Name   string
	Exists bool
}

func (n *Node) writeMetadata() (err error) {
	//ensure ID has been generated
	n.ID()
	nodePath := n.InternalPath()
	if n.owner == nil {
		if err = xattr.Set(nodePath, ownerIDAttr, []byte("")); err != nil {
			return errors.Wrap(err, "posixfs: could not set empty owner id attribute")
		}
		if err = xattr.Set(nodePath, ownerIDPAttr, []byte("")); err != nil {
			return errors.Wrap(err, "posixfs: could not set empty owner idp attribute")
		}
	} else {
		if err = xattr.Set(nodePath, ownerIDAttr, []byte(n.owner.OpaqueId)); err != nil {
			return errors.Wrap(err, "posixfs: could not set owner id attribute")
		}
		if err = xattr.Set(nodePath, ownerIDPAttr, []byte(n.owner.Idp)); err != nil {
			return errors.Wrap(err, "posixfs: could not set owner idp attribute")
		}
	}
	return
}

// ReadRecycleItem reads a recycle item as a node
// TODO refactor the returned params into Node properties? would make all the path transformations go away...
func ReadRecycleItem(ctx context.Context, lu *Lookup, key string) (n *Node, trashItem string, deletedNodePath string, origin string, err error) {
	return nil, "", "", "", errtypes.NotSupported("ReadRecycleItem")
}

// ReadNode creates a new instance from an id and checks if it exists
func ReadNode(ctx context.Context, lu *Lookup, id string) (n *Node, err error) {
	n = &Node{
		lu: lu,
	}
	parts := strings.SplitN(id, ":", 2)
	if len(parts) != 2 {
		return nil, errtypes.BadRequest("invalid file id")
	}
	command := []string{"find", filepath.Join(lu.Options.Root), "-inum", parts[0]}
	out, err := exec.Command(command[0], command[1:]...).Output()
	if err != nil {
		return nil, errtypes.InternalError(err.Error())
	}

	// TODO check we only got one path?
	path := string(out)
	n.Dir, n.Name = filepath.Split(path)

	if id != n.ID() {
		// TODO if not check if the id or the inode matches?
		log.Debug().Interface("node", n).Msg("id mismatch")
		n.Exists = false // return not existing node
		return &Node{id: id, lu: lu}, nil
	}

	return n, nil // swallow mismatch

}

// Child returns the child node with the given name
func (n *Node) Child(name string) (c *Node, err error) {
	c = &Node{
		lu:   n.lu,
		Dir:  filepath.Join(n.Dir, n.Name),
		Name: name,
	}

	if _, err := os.Stat(c.InternalPath()); err == nil {
		c.Exists = true
	}
	// TODO check if metadata still matches inode:uuid
	return
}

// Parent returns the parent node
func (n *Node) Parent() (p *Node, err error) {
	p = &Node{
		lu: n.lu,
	}
	p.Dir, p.Name = filepath.Split(n.Dir)
	p.Dir = strings.TrimSuffix(p.Dir, "/")

	if _, err := os.Stat(p.InternalPath()); err == nil {
		p.Exists = true
	}
	// TODO check if metadata still matches inode:uuid
	return
}

// Owner returns the cached owner id or reads it from the extended attributes
// TODO can be private as only the AsResourceInfo uses it
func (n *Node) Owner() (o *userpb.UserId, err error) {
	if n.owner != nil {
		return n.owner, nil
	}

	// FIXME ... do we return the owner of the reference or the owner of the target?
	// we don't really know the owner of the target ... and as the reference may point anywhere we cannot really find out
	// but what are the permissions? all? none? the gateway has to fill in?
	// TODO what if this is a reference?
	nodePath := n.InternalPath()
	// lookup parent id in extended attributes
	var attrBytes []byte
	// lookup name in extended attributes
	if attrBytes, err = xattr.Get(nodePath, ownerIDAttr); err == nil {
		if n.owner == nil {
			n.owner = &userpb.UserId{}
		}
		n.owner.OpaqueId = string(attrBytes)
	} else {
		return
	}
	// lookup name in extended attributes
	if attrBytes, err = xattr.Get(nodePath, ownerIDPAttr); err == nil {
		if n.owner == nil {
			n.owner = &userpb.UserId{}
		}
		n.owner.Idp = string(attrBytes)
	} else {
		return
	}
	return n.owner, err
}

// PermissionSet returns the permission set for the current user
// the parent nodes are not taken into account
func (n *Node) PermissionSet(ctx context.Context) *provider.ResourcePermissions {
	u, ok := user.ContextGetUser(ctx)
	if !ok {
		appctx.GetLogger(ctx).Debug().Interface("node", n).Msg("no user in context, returning default permissions")
		return noPermissions
	}
	if o, _ := n.Owner(); isSameUserID(u.Id, o) {
		return ownerPermissions
	}
	// read the permissions for the current user from the acls of the current node
	if np, err := n.ReadUserPermissions(ctx, u); err == nil {
		return np
	}
	return noPermissions
}

// calculateEtag returns a hash of fileid + tmtime (or mtime)
func calculateEtag(nodeID string, tmTime time.Time) (string, error) {
	h := md5.New()
	if _, err := io.WriteString(h, nodeID); err != nil {
		return "", err
	}
	if tb, err := tmTime.UTC().MarshalBinary(); err == nil {
		if _, err := h.Write(tb); err != nil {
			return "", err
		}
	} else {
		return "", err
	}
	return fmt.Sprintf(`"%x"`, h.Sum(nil)), nil
}

// SetMtime sets the mtime and atime of a node
func (n *Node) SetMtime(ctx context.Context, mtime string) error {
	sublog := appctx.GetLogger(ctx).With().Interface("node", n).Logger()
	if mt, err := parseMTime(mtime); err == nil {
		nodePath := n.InternalPath()
		// updating mtime also updates atime
		if err := os.Chtimes(nodePath, mt, mt); err != nil {
			sublog.Error().Err(err).
				Time("mtime", mt).
				Msg("could not set mtime")
			return errors.Wrap(err, "could not set mtime")
		}
	} else {
		sublog.Error().Err(err).
			Str("mtime", mtime).
			Msg("could not parse mtime")
		return errors.Wrap(err, "could not parse mtime")
	}
	return nil
}

// SetEtag sets the temporary etag of a node if it differs from the current etag
func (n *Node) SetEtag(ctx context.Context, val string) (err error) {
	sublog := appctx.GetLogger(ctx).With().Interface("node", n).Logger()
	nodePath := n.InternalPath()
	var tmTime time.Time
	if tmTime, err = n.GetTMTime(); err != nil {
		// no tmtime, use mtime
		var fi os.FileInfo
		if fi, err = os.Lstat(nodePath); err != nil {
			return
		}
		tmTime = fi.ModTime()
	}
	var etag string
	if etag, err = calculateEtag(n.ID(), tmTime); err != nil {
		return
	}

	// sanitize etag
	val = fmt.Sprintf("\"%s\"", strings.Trim(val, "\""))
	if etag == val {
		sublog.Debug().
			Str("etag", val).
			Msg("ignoring request to update identical etag")
		return nil
	}
	// etag is only valid until the calculated etag changes, is part of propagation
	return xattr.Set(nodePath, tmpEtagAttr, []byte(val))
}

// SetFavorite sets the favorite for the current user
// TODO we should not mess with the user here ... the favorites is now a user specific property for a file
// that cannot be mapped to extended attributes without leaking who has marked a file as a favorite
// it is a specific case of a tag, which is user individual as well
// TODO there are different types of tags
// 1. public that are managed by everyone
// 2. private tags that are only visible to the user
// 3. system tags that are only visible to the system
// 4. group tags that are only visible to a group ...
// urgh ... well this can be solved using different namespaces
// 1. public = p:
// 2. private = u:<uid>: for user specific
// 3. system = s: for system
// 4. group = g:<gid>:
// 5. app? = a:<aid>: for apps?
// obviously this only is secure when the u/s/g/a namespaces are not accessible by users in the filesystem
// public tags can be mapped to extended attributes
func (n *Node) SetFavorite(uid *userpb.UserId, val string) error {
	nodePath := n.InternalPath()
	// the favorite flag is specific to the user, so we need to incorporate the userid
	fa := fmt.Sprintf("%s%s@%s", favPrefix, uid.GetOpaqueId(), uid.GetIdp())
	return xattr.Set(nodePath, fa, []byte(val))
}

// AsResourceInfo return the node as CS3 ResourceInfo
func (n *Node) AsResourceInfo(ctx context.Context, rp *provider.ResourcePermissions, mdKeys []string) (ri *provider.ResourceInfo, err error) {
	sublog := appctx.GetLogger(ctx).With().Interface("node", n).Logger()

	var fn string
	nodePath := n.InternalPath()

	var fi os.FileInfo

	nodeType := provider.ResourceType_RESOURCE_TYPE_INVALID
	if fi, err = os.Lstat(nodePath); err != nil {
		return
	}

	var target []byte
	switch {
	case fi.IsDir():
		if target, err = xattr.Get(nodePath, referenceAttr); err == nil {
			nodeType = provider.ResourceType_RESOURCE_TYPE_REFERENCE
		} else {
			nodeType = provider.ResourceType_RESOURCE_TYPE_CONTAINER
		}
	case fi.Mode().IsRegular():
		nodeType = provider.ResourceType_RESOURCE_TYPE_FILE
	case fi.Mode()&os.ModeSymlink != 0:
		nodeType = provider.ResourceType_RESOURCE_TYPE_SYMLINK
		// TODO reference using ext attr on a symlink
		// nodeType = provider.ResourceType_RESOURCE_TYPE_REFERENCE
	}

	id := &provider.ResourceId{OpaqueId: n.ID()}

	fn, err = n.lu.Path(ctx, n)
	if err != nil {
		return nil, err
	}

	ri = &provider.ResourceInfo{
		Id:            id,
		Path:          fn,
		Type:          nodeType,
		MimeType:      mime.Detect(nodeType == provider.ResourceType_RESOURCE_TYPE_CONTAINER, fn),
		Size:          uint64(fi.Size()),
		Target:        string(target),
		PermissionSet: rp,
	}

	if ri.Owner, err = n.Owner(); err != nil {
		sublog.Debug().Err(err).Msg("could not determine owner")
	}

	// TODO make etag of files use fileid and checksum

	var tmTime time.Time
	if tmTime, err = n.GetTMTime(); err != nil {
		// no tmtime, use mtime
		tmTime = fi.ModTime()
	}

	// use temporary etag if it is set
	if b, err := xattr.Get(nodePath, tmpEtagAttr); err == nil {
		ri.Etag = fmt.Sprintf(`"%x"`, string(b)) // TODO why do we convert string(b)? is the temporary etag stored as string? -> should we use bytes? use hex.EncodeToString?
	} else if ri.Etag, err = calculateEtag(n.ID(), tmTime); err != nil {
		sublog.Debug().Err(err).Msg("could not calculate etag")
	}

	// mtime uses tmtime if present
	// TODO expose mtime and tmtime separately?
	un := tmTime.UnixNano()
	ri.Mtime = &types.Timestamp{
		Seconds: uint64(un / 1000000000),
		Nanos:   uint32(un % 1000000000),
	}

	mdKeysMap := make(map[string]struct{})
	for _, k := range mdKeys {
		mdKeysMap[k] = struct{}{}
	}

	var returnAllKeys bool
	if _, ok := mdKeysMap["*"]; len(mdKeys) == 0 || ok {
		returnAllKeys = true
	}

	metadata := map[string]string{}

	// read favorite flag for the current user
	if _, ok := mdKeysMap[_favoriteKey]; returnAllKeys || ok {
		favorite := ""
		if u, ok := user.ContextGetUser(ctx); ok {
			// the favorite flag is specific to the user, so we need to incorporate the userid
			if uid := u.GetId(); uid != nil {
				fa := fmt.Sprintf("%s%s@%s", favPrefix, uid.GetOpaqueId(), uid.GetIdp())
				if val, err := xattr.Get(nodePath, fa); err == nil {
					sublog.Debug().
						Str("favorite", fa).
						Msg("found favorite flag")
					favorite = string(val)
				}
			} else {
				sublog.Error().Err(errtypes.UserRequired("userrequired")).Msg("user has no id")
			}
		} else {
			sublog.Error().Err(errtypes.UserRequired("userrequired")).Msg("error getting user from ctx")
		}
		metadata[_favoriteKey] = favorite
	}

	// share indicator
	if _, ok := mdKeysMap[_shareTypesKey]; returnAllKeys || ok {
		if n.hasUserShares(ctx) {
			metadata[_shareTypesKey] = _userShareType
		}
	}

	// checksums
	if _, ok := mdKeysMap[_checksumsKey]; (nodeType == provider.ResourceType_RESOURCE_TYPE_FILE) && returnAllKeys || ok {
		// TODO which checksum was requested? sha1 adler32 or md5? for now hardcode sha1?
		readChecksumIntoResourceChecksum(ctx, nodePath, storageprovider.XSSHA1, ri)
		readChecksumIntoOpaque(ctx, nodePath, storageprovider.XSMD5, ri)
		readChecksumIntoOpaque(ctx, nodePath, storageprovider.XSAdler32, ri)
	}

	// only read the requested metadata attributes
	attrs, err := xattr.List(nodePath)
	if err != nil {
		sublog.Error().Err(err).Msg("error getting list of extended attributes")
	} else {
		for i := range attrs {
			// filter out non-custom properties
			if !strings.HasPrefix(attrs[i], metadataPrefix) {
				continue
			}
			// only read when key was requested
			k := attrs[i][len(metadataPrefix):]
			if _, ok := mdKeysMap[k]; returnAllKeys || ok {
				if val, err := xattr.Get(nodePath, attrs[i]); err == nil {
					metadata[k] = string(val)
				} else {
					sublog.Error().Err(err).
						Str("entry", attrs[i]).
						Msg("error retrieving xattr metadata")
				}
			}

		}
	}
	ri.ArbitraryMetadata = &provider.ArbitraryMetadata{
		Metadata: metadata,
	}

	sublog.Debug().
		Interface("ri", ri).
		Msg("AsResourceInfo")

	return ri, nil
}

func readChecksumIntoResourceChecksum(ctx context.Context, nodePath, algo string, ri *provider.ResourceInfo) {
	v, err := xattr.Get(nodePath, checksumPrefix+algo)
	switch {
	case err == nil:
		ri.Checksum = &provider.ResourceChecksum{
			Type: storageprovider.PKG2GRPCXS(algo),
			Sum:  hex.EncodeToString(v),
		}
	case isNoData(err):
		appctx.GetLogger(ctx).Debug().Err(err).Str("nodepath", nodePath).Str("algorithm", algo).Msg("checksum not set")
	case isNotFound(err):
		appctx.GetLogger(ctx).Error().Err(err).Str("nodepath", nodePath).Str("algorithm", algo).Msg("file not fount")
	default:
		appctx.GetLogger(ctx).Error().Err(err).Str("nodepath", nodePath).Str("algorithm", algo).Msg("could not read checksum")
	}
}
func readChecksumIntoOpaque(ctx context.Context, nodePath, algo string, ri *provider.ResourceInfo) {
	v, err := xattr.Get(nodePath, checksumPrefix+algo)
	switch {
	case err == nil:
		if ri.Opaque == nil {
			ri.Opaque = &types.Opaque{
				Map: map[string]*types.OpaqueEntry{},
			}
		}
		ri.Opaque.Map[algo] = &types.OpaqueEntry{
			Decoder: "plain",
			Value:   []byte(hex.EncodeToString(v)),
		}
	case isNoData(err):
		appctx.GetLogger(ctx).Debug().Err(err).Str("nodepath", nodePath).Str("algorithm", algo).Msg("checksum not set")
	case isNotFound(err):
		appctx.GetLogger(ctx).Error().Err(err).Str("nodepath", nodePath).Str("algorithm", algo).Msg("file not fount")
	default:
		appctx.GetLogger(ctx).Error().Err(err).Str("nodepath", nodePath).Str("algorithm", algo).Msg("could not read checksum")
	}
}

// HasPropagation checks if the propagation attribute exists and is set to "1"
func (n *Node) HasPropagation() (propagation bool) {
	if b, err := xattr.Get(n.InternalPath(), propagationAttr); err == nil {
		return string(b) == "1"
	}
	return false
}

// GetTMTime reads the tmtime from the extended attributes
func (n *Node) GetTMTime() (tmTime time.Time, err error) {
	var b []byte
	if b, err = xattr.Get(n.InternalPath(), treeMTimeAttr); err != nil {
		return
	}
	return time.Parse(time.RFC3339Nano, string(b))
}

// SetTMTime writes the tmtime to the extended attributes
func (n *Node) SetTMTime(t time.Time) (err error) {
	return xattr.Set(n.InternalPath(), treeMTimeAttr, []byte(t.UTC().Format(time.RFC3339Nano)))
}

// SetChecksum writes the checksum with the given checksum type to the extended attributes
func (n *Node) SetChecksum(csType string, h hash.Hash) (err error) {
	return xattr.Set(n.InternalPath(), checksumPrefix+csType, h.Sum(nil))
}

// UnsetTempEtag removes the temporary etag attribute
func (n *Node) UnsetTempEtag() (err error) {
	if err = xattr.Remove(n.InternalPath(), tmpEtagAttr); err != nil {
		if e, ok := err.(*xattr.Error); ok && (e.Err.Error() == "no data available" ||
			// darwin
			e.Err.Error() == "attribute not found") {
			return nil
		}
	}
	return err
}

// ReadUserPermissions will assemble the permissions for the current user on the given node without parent nodes
func (n *Node) ReadUserPermissions(ctx context.Context, u *userpb.User) (ap *provider.ResourcePermissions, err error) {
	// check if the current user is the owner
	o, err := n.Owner()
	if err != nil {
		// TODO check if a parent folder has the owner set?
		appctx.GetLogger(ctx).Error().Err(err).Interface("node", n).Msg("could not determine owner, returning default permissions")
		return noPermissions, err
	}
	if o.OpaqueId == "" {
		// this happens for root nodes in the storage. the extended attributes are set to emptystring to indicate: no owner
		// TODO what if no owner is set but grants are present?
		return noOwnerPermissions, nil
	}
	if isSameUserID(u.Id, o) {
		appctx.GetLogger(ctx).Debug().Interface("node", n).Msg("user is owner, returning owner permissions")
		return ownerPermissions, nil
	}

	ap = &provider.ResourcePermissions{}

	// for an efficient group lookup convert the list of groups to a map
	// groups are just strings ... groupnames ... or group ids ??? AAARGH !!!
	groupsMap := make(map[string]bool, len(u.Groups))
	for i := range u.Groups {
		groupsMap[u.Groups[i]] = true
	}

	var g *provider.Grant

	// we read all grantees from the node
	var grantees []string
	if grantees, err = n.ListGrantees(ctx); err != nil {
		appctx.GetLogger(ctx).Error().Err(err).Interface("node", n).Msg("error listing grantees")
		return nil, err
	}

	// instead of making n getxattr syscalls we are going to list the acls and filter them here
	// we have two options here:
	// 1. we can start iterating over the acls / grants on the node or
	// 2. we can iterate over the number of groups
	// The current implementation tries to be defensive for cases where users have hundreds or thousands of groups, so we iterate over the existing acls.
	userace := grantPrefix + _userAcePrefix + u.Id.OpaqueId
	userFound := false
	for i := range grantees {
		switch {
		// we only need to find the user once
		case !userFound && grantees[i] == userace:
			g, err = n.ReadGrant(ctx, grantees[i])
		case strings.HasPrefix(grantees[i], grantPrefix+_groupAcePrefix): // only check group grantees
			gr := strings.TrimPrefix(grantees[i], grantPrefix+_groupAcePrefix)
			if groupsMap[gr] {
				g, err = n.ReadGrant(ctx, grantees[i])
			} else {
				// no need to check attribute
				continue
			}
		default:
			// no need to check attribute
			continue
		}

		switch {
		case err == nil:
			addPermissions(ap, g.GetPermissions())
		case isNoData(err):
			err = nil
			appctx.GetLogger(ctx).Error().Interface("node", n).Str("grant", grantees[i]).Interface("grantees", grantees).Msg("grant vanished from node after listing")
			// continue with next segment
		default:
			appctx.GetLogger(ctx).Error().Err(err).Interface("node", n).Str("grant", grantees[i]).Msg("error reading permissions")
			// continue with next segment
		}
	}

	appctx.GetLogger(ctx).Debug().Interface("permissions", ap).Interface("node", n).Interface("user", u).Msg("returning aggregated permissions")
	return ap, nil
}

// ListGrantees lists the grantees of the current node
// We don't want to wast time and memory by creating grantee objects.
// The function will return a list of opaque strings that can be used to make a ReadGrant call
func (n *Node) ListGrantees(ctx context.Context) (grantees []string, err error) {
	var attrs []string
	if attrs, err = xattr.List(n.InternalPath()); err != nil {
		appctx.GetLogger(ctx).Error().Err(err).Interface("node", n).Msg("error listing attributes")
		return nil, err
	}
	for i := range attrs {
		if strings.HasPrefix(attrs[i], grantPrefix) {
			grantees = append(grantees, attrs[i])
		}
	}
	return
}

// ReadGrant reads a CS3 grant
func (n *Node) ReadGrant(ctx context.Context, grantee string) (g *provider.Grant, err error) {
	var b []byte
	if b, err = xattr.Get(n.InternalPath(), grantee); err != nil {
		return nil, err
	}
	var e *ace.ACE
	if e, err = ace.Unmarshal(strings.TrimPrefix(grantee, grantPrefix), b); err != nil {
		return nil, err
	}
	return e.Grant(), nil
}

func (n *Node) hasUserShares(ctx context.Context) bool {
	g, err := n.ListGrantees(ctx)
	if err != nil {
		appctx.GetLogger(ctx).Error().Err(err).Msg("hasUserShares: listGrantees")
		return false
	}

	for i := range g {
		if strings.Contains(g[i], grantPrefix+_userAcePrefix) {
			return true
		}
	}
	return false
}

func (n *Node) InternalPath() string {
	return filepath.Join(n.lu.Options.Root, n.Dir, n.Name)
}

func (n *Node) ID() string {
	if n.id != "" {
		return n.id
	}
	attrBytes, err := xattr.Get(n.InternalPath(), idAttr)
	switch {
	case err == nil:
		n.Exists = true
		n.id = string(attrBytes)
	case isNoData(err):
		n.Exists = true
		// try to get inode
		var md os.FileInfo
		var ino string
		if md, err = os.Stat(n.InternalPath()); err != nil {
			if stat, ok := md.Sys().(*syscall.Stat_t); ok {
				ino = fmt.Sprintf("%d", stat.Ino)
			}
		}
		n.id = fmt.Sprintf("%s:%s", ino, uuid.New().String())
		// try to store it
		_ = xattr.Set(n.InternalPath(), idAttr, []byte(n.id))
	case isNotFound(err):
		n.Exists = false
		// ID might be empty, and the above layers should be able to deal with an empty ID ... or do we fall back to the string?
	default:
		// ID might be empty, and the above layers should be able to deal with an empty ID ... or do we fall back to the string?
	}
	return n.id
}
