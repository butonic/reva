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

package usershareprovider

import (
	"context"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	gateway "github.com/cs3org/go-cs3apis/cs3/gateway/v1beta1"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	collaboration "github.com/cs3org/go-cs3apis/cs3/sharing/collaboration/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/cs3org/reva/v2/pkg/appctx"
	"github.com/cs3org/reva/v2/pkg/conversions"
	ctxpkg "github.com/cs3org/reva/v2/pkg/ctx"
	"github.com/cs3org/reva/v2/pkg/errtypes"
	"github.com/cs3org/reva/v2/pkg/permission"
	"github.com/cs3org/reva/v2/pkg/rgrpc"
	"github.com/cs3org/reva/v2/pkg/rgrpc/status"
	"github.com/cs3org/reva/v2/pkg/rgrpc/todo/pool"
	"github.com/cs3org/reva/v2/pkg/share"
	"github.com/cs3org/reva/v2/pkg/share/manager/registry"
	"github.com/cs3org/reva/v2/pkg/sharedconf"
	"github.com/cs3org/reva/v2/pkg/utils"
)

const (
	_fieldMaskPathMountPoint  = "mount_point"
	_fieldMaskPathPermissions = "permissions"
)

func init() {
	rgrpc.Register("usershareprovider", NewDefault)
}

type config struct {
	Driver                string                            `mapstructure:"driver"`
	Drivers               map[string]map[string]interface{} `mapstructure:"drivers"`
	GatewayAddr           string                            `mapstructure:"gateway_addr"`
	AllowedPathsForShares []string                          `mapstructure:"allowed_paths_for_shares"`
}

func (c *config) init() {
	if c.Driver == "" {
		c.Driver = "json"
	}
}

type service struct {
	sm                    share.Manager
	gatewaySelector       pool.Selectable[gateway.GatewayAPIClient]
	allowedPathsForShares []*regexp.Regexp
}

func getShareManager(c *config) (share.Manager, error) {
	if f, ok := registry.NewFuncs[c.Driver]; ok {
		return f(c.Drivers[c.Driver])
	}
	return nil, errtypes.NotFound("driver not found: " + c.Driver)
}

// TODO(labkode): add ctx to Close.
func (s *service) Close() error {
	return nil
}

func (s *service) UnprotectedEndpoints() []string {
	return []string{}
}

func (s *service) Register(ss *grpc.Server) {
	collaboration.RegisterCollaborationAPIServer(ss, s)
}

func parseConfig(m map[string]interface{}) (*config, error) {
	c := &config{}
	if err := mapstructure.Decode(m, c); err != nil {
		err = errors.Wrap(err, "error decoding conf")
		return nil, err
	}
	return c, nil
}

// New creates a new user share provider svc initialized from defaults
func NewDefault(m map[string]interface{}, ss *grpc.Server) (rgrpc.Service, error) {

	c, err := parseConfig(m)
	if err != nil {
		return nil, err
	}

	c.init()

	sm, err := getShareManager(c)
	if err != nil {
		return nil, err
	}

	allowedPathsForShares := make([]*regexp.Regexp, 0, len(c.AllowedPathsForShares))
	for _, s := range c.AllowedPathsForShares {
		regex, err := regexp.Compile(s)
		if err != nil {
			return nil, err
		}
		allowedPathsForShares = append(allowedPathsForShares, regex)
	}

	gatewaySelector, err := pool.GatewaySelector(sharedconf.GetGatewaySVC(c.GatewayAddr))
	if err != nil {
		return nil, err
	}

	return New(gatewaySelector, sm, allowedPathsForShares), nil
}

// New creates a new user share provider svc
func New(gatewaySelector pool.Selectable[gateway.GatewayAPIClient], sm share.Manager, allowedPathsForShares []*regexp.Regexp) rgrpc.Service {
	service := &service{
		sm:                    sm,
		gatewaySelector:       gatewaySelector,
		allowedPathsForShares: allowedPathsForShares,
	}

	return service
}

func (s *service) isPathAllowed(path string) bool {
	if len(s.allowedPathsForShares) == 0 {
		return true
	}
	for _, reg := range s.allowedPathsForShares {
		if reg.MatchString(path) {
			return true
		}
	}
	return false
}

func (s *service) CreateShare(ctx context.Context, req *collaboration.CreateShareRequest) (*collaboration.CreateShareResponse, error) {
	log := appctx.GetLogger(ctx)
	user := ctxpkg.ContextMustGetUser(ctx)

	// Grants must not allow grant permissions
	if HasGrantPermissions(req.GetGrant().GetPermissions().GetPermissions()) {
		return &collaboration.CreateShareResponse{
			Status: status.NewInvalidArg(ctx, "resharing not supported"),
		}, nil
	}

	gatewayClient, err := s.gatewaySelector.Next()
	if err != nil {
		return nil, err
	}

	// check if the user has the permission to create shares at all
	ok, err := utils.CheckPermission(ctx, permission.WriteShare, gatewayClient)
	if err != nil {
		return &collaboration.CreateShareResponse{
			Status: status.NewInternal(ctx, "failed check user permission to write public link"),
		}, err
	}
	if !ok {
		return &collaboration.CreateShareResponse{
			Status: status.NewPermissionDenied(ctx, nil, "no permission to create public links"),
		}, nil
	}

	if req.GetGrant().GetGrantee().GetType() == provider.GranteeType_GRANTEE_TYPE_USER && req.GetGrant().GetGrantee().GetUserId().GetIdp() == "" {
		// use logged in user Idp as default.
		req.GetGrant().GetGrantee().Id = &provider.Grantee_UserId{
			UserId: &userpb.UserId{
				OpaqueId: req.GetGrant().GetGrantee().GetUserId().GetOpaqueId(),
				Idp:      user.GetId().GetIdp(),
				Type:     userpb.UserType_USER_TYPE_PRIMARY},
		}
	}

	sRes, err := gatewayClient.Stat(ctx, &provider.StatRequest{Ref: &provider.Reference{ResourceId: req.GetResourceInfo().GetId()}})
	if err != nil {
		log.Err(err).Interface("resource_id", req.GetResourceInfo().GetId()).Msg("failed to stat resource to share")
		return &collaboration.CreateShareResponse{
			Status: status.NewInternal(ctx, "failed to stat shared resource"),
		}, err
	}
	// the user needs to have the AddGrant permissions on the Resource to be able to create a share
	if !sRes.GetInfo().GetPermissionSet().AddGrant {
		return &collaboration.CreateShareResponse{
			Status: status.NewPermissionDenied(ctx, nil, "no permission to add grants on shared resource"),
		}, err
	}
	// check if the share creator has sufficient permissions to do so.
	if shareCreationAllowed := conversions.SufficientCS3Permissions(
		sRes.GetInfo().GetPermissionSet(),
		req.GetGrant().GetPermissions().GetPermissions(),
	); !shareCreationAllowed {
		return &collaboration.CreateShareResponse{
			Status: status.NewPermissionDenied(ctx, nil, "insufficient permissions to create that kind of share"),
		}, nil
	}
	// check if the requested permission are plausible for the Resource
	if sRes.GetInfo().GetType() == provider.ResourceType_RESOURCE_TYPE_FILE {
		if newPermissions := req.GetGrant().GetPermissions().GetPermissions(); newPermissions.GetCreateContainer() || newPermissions.GetMove() || newPermissions.GetDelete() {
			return &collaboration.CreateShareResponse{
				Status: status.NewInvalid(ctx, "cannot set the requested permissions on that type of resource"),
			}, nil
		}
	}

	if !s.isPathAllowed(req.GetResourceInfo().GetPath()) {
		return &collaboration.CreateShareResponse{
			Status: status.NewFailedPrecondition(ctx, nil, "share creation is not allowed for the specified path"),
		}, nil
	}

	createdShare, err := s.sm.Share(ctx, req.GetResourceInfo(), req.GetGrant())
	if err != nil {
		return &collaboration.CreateShareResponse{
			Status: status.NewStatusFromErrType(ctx, "error creating share", err),
		}, nil
	}

	return &collaboration.CreateShareResponse{
		Status: status.NewOK(ctx),
		Share:  createdShare,
	}, nil
}

func HasGrantPermissions(p *provider.ResourcePermissions) bool {
	return p.GetAddGrant() || p.GetUpdateGrant() || p.GetRemoveGrant() || p.GetDenyGrant()
}

func (s *service) RemoveShare(ctx context.Context, req *collaboration.RemoveShareRequest) (*collaboration.RemoveShareResponse, error) {
	log := appctx.GetLogger(ctx)
	user := ctxpkg.ContextMustGetUser(ctx)
	share, err := s.sm.GetShare(ctx, req.Ref)
	if err != nil {
		return &collaboration.RemoveShareResponse{
			Status: status.NewInternal(ctx, "error getting share"),
		}, nil
	}

	gatewayClient, err := s.gatewaySelector.Next()
	if err != nil {
		return nil, err
	}
	sRes, err := gatewayClient.Stat(ctx, &provider.StatRequest{Ref: &provider.Reference{ResourceId: share.GetResourceId()}})
	if err != nil {
		log.Err(err).Interface("resource_id", share.GetResourceId()).Msg("failed to stat shared resource")
		return &collaboration.RemoveShareResponse{
			Status: status.NewInternal(ctx, "failed to stat shared resource"),
		}, err
	}
	// the requesting user needs to be either the Owner/Creator of the share or have the RemoveGrant permissions on the Resource
	switch {
	case utils.UserEqual(user.GetId(), share.GetCreator()) || utils.UserEqual(user.GetId(), share.GetOwner()):
		fallthrough
	case sRes.GetInfo().GetPermissionSet().RemoveGrant:
		break
	default:
		return &collaboration.RemoveShareResponse{
			Status: status.NewPermissionDenied(ctx, nil, "no permission to remove grants on shared resource"),
		}, err
	}

	err = s.sm.Unshare(ctx, req.Ref)
	if err != nil {
		return &collaboration.RemoveShareResponse{
			Status: status.NewInternal(ctx, "error removing share"),
		}, nil
	}

	o := utils.AppendJSONToOpaque(nil, "resourceid", share.GetResourceId())
	if user := share.GetGrantee().GetUserId(); user != nil {
		o = utils.AppendJSONToOpaque(o, "granteeuserid", user)
	} else {
		o = utils.AppendJSONToOpaque(o, "granteegroupid", share.GetGrantee().GetGroupId())
	}

	return &collaboration.RemoveShareResponse{
		Opaque: o,
		Status: status.NewOK(ctx),
	}, nil
}

func (s *service) GetShare(ctx context.Context, req *collaboration.GetShareRequest) (*collaboration.GetShareResponse, error) {
	share, err := s.sm.GetShare(ctx, req.Ref)
	if err != nil {
		var st *rpc.Status
		switch err.(type) {
		case errtypes.IsNotFound:
			st = status.NewNotFound(ctx, err.Error())
		default:
			st = status.NewInternal(ctx, err.Error())
		}
		return &collaboration.GetShareResponse{
			Status: st,
		}, nil
	}

	return &collaboration.GetShareResponse{
		Status: status.NewOK(ctx),
		Share:  share,
	}, nil
}

func (s *service) ListShares(ctx context.Context, req *collaboration.ListSharesRequest) (*collaboration.ListSharesResponse, error) {
	shares, err := s.sm.ListShares(ctx, req.Filters) // TODO(labkode): add filter to share manager
	if err != nil {
		return &collaboration.ListSharesResponse{
			Status: status.NewInternal(ctx, "error listing shares"),
		}, nil
	}

	res := &collaboration.ListSharesResponse{
		Status: status.NewOK(ctx),
		Shares: shares,
	}
	return res, nil
}

func (s *service) UpdateShare(ctx context.Context, req *collaboration.UpdateShareRequest) (*collaboration.UpdateShareResponse, error) {
	log := appctx.GetLogger(ctx)
	user := ctxpkg.ContextMustGetUser(ctx)

	// Grants must not allow grant permissions
	if HasGrantPermissions(req.GetShare().GetPermissions().GetPermissions()) {
		return &collaboration.UpdateShareResponse{
			Status: status.NewInvalidArg(ctx, "resharing not supported"),
		}, nil
	}

	gatewayClient, err := s.gatewaySelector.Next()
	if err != nil {
		return nil, err
	}

	// check if the user has the permission to create shares at all
	ok, err := utils.CheckPermission(ctx, permission.WriteShare, gatewayClient)
	if err != nil {
		return &collaboration.UpdateShareResponse{
			Status: status.NewInternal(ctx, "failed check user permission to write share"),
		}, err
	}
	if !ok {
		return &collaboration.UpdateShareResponse{
			Status: status.NewPermissionDenied(ctx, nil, "no permission to create user share"),
		}, nil
	}

	// Read share from backend. We need the shared resource's id for STATing it, it might not be in
	// the incoming request
	currentShare, err := s.sm.GetShare(ctx,
		&collaboration.ShareReference{
			Spec: &collaboration.ShareReference_Id{
				Id: req.GetShare().GetId(),
			},
		},
	)
	if err != nil {
		var st *rpc.Status
		switch err.(type) {
		case errtypes.IsNotFound:
			st = status.NewNotFound(ctx, err.Error())
		default:
			st = status.NewInternal(ctx, err.Error())
		}
		return &collaboration.UpdateShareResponse{
			Status: st,
		}, nil
	}

	sRes, err := gatewayClient.Stat(ctx, &provider.StatRequest{Ref: &provider.Reference{ResourceId: currentShare.GetResourceId()}})
	if err != nil {
		log.Err(err).Interface("resource_id", req.GetShare().GetResourceId()).Msg("failed to stat resource to share")
		return &collaboration.UpdateShareResponse{
			Status: status.NewInternal(ctx, "failed to stat shared resource"),
		}, err
	}
	// the requesting user needs to be either the Owner/Creator of the share or have the UpdateGrant permissions on the Resource
	switch {
	case utils.UserEqual(user.GetId(), currentShare.GetCreator()) || utils.UserEqual(user.GetId(), currentShare.GetOwner()):
		fallthrough
	case sRes.GetInfo().GetPermissionSet().UpdateGrant:
		break
	default:
		return &collaboration.UpdateShareResponse{
			Status: status.NewPermissionDenied(ctx, nil, "no permission to remove grants on shared resource"),
		}, err
	}

	// If this is a permissions update, check if user's permissions on the resource are sufficient to set the desired permissions
	var newPermissions *provider.ResourcePermissions
	if slices.Contains(req.GetUpdateMask().GetPaths(), _fieldMaskPathPermissions) {
		newPermissions = req.GetShare().GetPermissions().GetPermissions()
	} else {
		newPermissions = req.GetField().GetPermissions().GetPermissions()
	}
	if newPermissions != nil && !conversions.SufficientCS3Permissions(sRes.GetInfo().GetPermissionSet(), newPermissions) {
		return &collaboration.UpdateShareResponse{
			Status: status.NewPermissionDenied(ctx, nil, "insufficient permissions to create that kind of share"),
		}, nil
	}

	// check if the requested permission are plausible for the Resource
	// do we need more here?
	if sRes.GetInfo().GetType() == provider.ResourceType_RESOURCE_TYPE_FILE {
		if newPermissions.GetCreateContainer() || newPermissions.GetMove() || newPermissions.GetDelete() {
			return &collaboration.UpdateShareResponse{
				Status: status.NewInvalid(ctx, "cannot set the requested permissions on that type of resource"),
			}, nil
		}
	}

	share, err := s.sm.UpdateShare(ctx, req.Ref, req.Field.GetPermissions(), req.Share, req.UpdateMask) // TODO(labkode): check what to update
	if err != nil {
		return &collaboration.UpdateShareResponse{
			Status: status.NewInternal(ctx, "error updating share"),
		}, nil
	}

	res := &collaboration.UpdateShareResponse{
		Status: status.NewOK(ctx),
		Share:  share,
	}
	return res, nil
}

func (s *service) ListReceivedShares(ctx context.Context, req *collaboration.ListReceivedSharesRequest) (*collaboration.ListReceivedSharesResponse, error) {
	// For the UI add a filter to not display the denial shares
	foundExclude := false
	for _, f := range req.Filters {
		if f.Type == collaboration.Filter_TYPE_EXCLUDE_DENIALS {
			foundExclude = true
			break
		}
	}
	if !foundExclude {
		req.Filters = append(req.Filters, &collaboration.Filter{Type: collaboration.Filter_TYPE_EXCLUDE_DENIALS})
	}

	var uid userpb.UserId
	_ = utils.ReadJSONFromOpaque(req.Opaque, "userid", &uid)
	shares, err := s.sm.ListReceivedShares(ctx, req.Filters, &uid) // TODO(labkode): check what to update
	if err != nil {
		return &collaboration.ListReceivedSharesResponse{
			Status: status.NewInternal(ctx, "error listing received shares"),
		}, nil
	}

	res := &collaboration.ListReceivedSharesResponse{
		Status: status.NewOK(ctx),
		Shares: shares,
	}
	return res, nil
}

func (s *service) GetReceivedShare(ctx context.Context, req *collaboration.GetReceivedShareRequest) (*collaboration.GetReceivedShareResponse, error) {
	log := appctx.GetLogger(ctx)

	share, err := s.sm.GetReceivedShare(ctx, req.Ref)
	if err != nil {
		log.Err(err).Msg("error getting received share")
		switch err.(type) {
		case errtypes.NotFound:
			return &collaboration.GetReceivedShareResponse{
				Status: status.NewNotFound(ctx, "error getting received share"),
			}, nil
		default:
			return &collaboration.GetReceivedShareResponse{
				Status: status.NewInternal(ctx, "error getting received share"),
			}, nil
		}
	}

	res := &collaboration.GetReceivedShareResponse{
		Status: status.NewOK(ctx),
		Share:  share,
	}
	return res, nil
}

func (s *service) UpdateReceivedShare(ctx context.Context, req *collaboration.UpdateReceivedShareRequest) (*collaboration.UpdateReceivedShareResponse, error) {
	if req.GetShare() == nil {
		return &collaboration.UpdateReceivedShareResponse{
			Status: status.NewInvalid(ctx, "updating requires a received share object"),
		}, nil
	}

	if req.GetShare().GetShare() == nil {
		return &collaboration.UpdateReceivedShareResponse{
			Status: status.NewInvalid(ctx, "share missing"),
		}, nil
	}

	if req.GetShare().GetShare().GetId() == nil {
		return &collaboration.UpdateReceivedShareResponse{
			Status: status.NewInvalid(ctx, "share id missing"),
		}, nil
	}

	if req.GetShare().GetShare().GetId().GetOpaqueId() == "" {
		return &collaboration.UpdateReceivedShareResponse{
			Status: status.NewInvalid(ctx, "share id empty"),
		}, nil
	}

	gatewayClient, err := s.gatewaySelector.Next()
	if err != nil {
		return nil, err
	}

	receivedShare, err := gatewayClient.GetReceivedShare(ctx, &collaboration.GetReceivedShareRequest{
		Ref: &collaboration.ShareReference{
			Spec: &collaboration.ShareReference_Id{
				Id: req.GetShare().GetShare().GetId(),
			},
		},
	})
	switch {
	case err != nil:
		fallthrough
	case receivedShare.GetStatus().GetCode() != rpc.Code_CODE_OK:
		return &collaboration.UpdateReceivedShareResponse{
			Status: receivedShare.GetStatus(),
		}, err
	}

	resourceStat, err := gatewayClient.Stat(ctx, &provider.StatRequest{
		Ref: &provider.Reference{
			ResourceId: receivedShare.GetShare().GetShare().GetResourceId(),
		},
	})
	switch {
	case err != nil:
		fallthrough
	case receivedShare.GetStatus().GetCode() != rpc.Code_CODE_OK:
		return &collaboration.UpdateReceivedShareResponse{
			Status: receivedShare.GetStatus(),
		}, err
	}

	// check if the update mask is nil and if so, initialize it
	if req.GetUpdateMask() == nil {
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{}}
	}

	// handle mount point related updates
	{
		// find a suitable mount point
		var requestedMountpoint string
		switch {
		case slices.Contains(req.GetUpdateMask().GetPaths(), _fieldMaskPathMountPoint) && req.GetShare().GetMountPoint().GetPath() != "":
			requestedMountpoint = req.GetShare().GetMountPoint().GetPath()
		case receivedShare.GetShare().GetMountPoint().GetPath() != "":
			requestedMountpoint = receivedShare.GetShare().GetMountPoint().GetPath()
		default:
			requestedMountpoint = resourceStat.GetInfo().GetName()
		}

		// check if the requested mount point is available and if not, find a suitable one
		availableMountpoint, _, err := GetMountpointAndUnmountedShares(ctx, gatewayClient,
			resourceStat.GetInfo().GetId(),
			requestedMountpoint,
		)
		if err != nil {
			return &collaboration.UpdateReceivedShareResponse{
				Status: status.NewInternal(ctx, err.Error()),
			}, nil
		}

		if !slices.Contains(req.GetUpdateMask().GetPaths(), _fieldMaskPathMountPoint) {
			req.GetUpdateMask().Paths = append(req.GetUpdateMask().GetPaths(), _fieldMaskPathMountPoint)
		}

		req.GetShare().MountPoint = &provider.Reference{
			Path: availableMountpoint,
		}
	}

	var uid userpb.UserId
	_ = utils.ReadJSONFromOpaque(req.Opaque, "userid", &uid)
	updatedShare, err := s.sm.UpdateReceivedShare(ctx, req.Share, req.UpdateMask, &uid)
	if err != nil {
		return &collaboration.UpdateReceivedShareResponse{
			Status: status.NewInternal(ctx, "error updating received share"),
		}, nil
	}

	return &collaboration.UpdateReceivedShareResponse{
		Status: status.NewOK(ctx),
		Share:  updatedShare,
	}, nil
}

// GetMountpointAndUnmountedShares returns a new or existing mountpoint for the given info and produces a list of unmounted received shares for the same resource
func GetMountpointAndUnmountedShares(ctx context.Context, gwc gateway.GatewayAPIClient, id *provider.ResourceId, name string) (string, []*collaboration.ReceivedShare, error) {
	var unmountedShares []*collaboration.ReceivedShare
	receivedShares, err := listReceivedShares(ctx, gwc)
	if err != nil {
		return "", unmountedShares, err
	}

	// we need to sort the received shares by mount point in order to make things easier to evaluate.
	base := filepath.Clean(name)
	mount := base
	existingMountpoint := ""
	mountedShares := make([]string, 0, len(receivedShares))
	var pathExists bool

	for _, s := range receivedShares {
		resourceIDEqual := utils.ResourceIDEqual(s.GetShare().GetResourceId(), id)

		if resourceIDEqual && s.State == collaboration.ShareState_SHARE_STATE_ACCEPTED {
			// a share to the resource already exists and is mounted, remembers the mount point
			_, err := utils.GetResourceByID(ctx, s.GetShare().GetResourceId(), gwc)
			if err == nil {
				existingMountpoint = s.GetMountPoint().GetPath()
			}
		}

		if resourceIDEqual && s.State != collaboration.ShareState_SHARE_STATE_ACCEPTED {
			// a share to the resource already exists but is not mounted, collect the unmounted share
			unmountedShares = append(unmountedShares, s)
		}

		if s.State == collaboration.ShareState_SHARE_STATE_ACCEPTED {
			// collect all accepted mount points
			mountedShares = append(mountedShares, s.GetMountPoint().GetPath())
			if s.GetMountPoint().GetPath() == mount {
				// does the shared resource still exist?
				_, err := utils.GetResourceByID(ctx, s.GetShare().GetResourceId(), gwc)
				if err == nil {
					pathExists = true
				}
				// TODO we could delete shares here if the stat returns code NOT FOUND ... but listening for file deletes would be better
			}
		}
	}

	if existingMountpoint != "" {
		// we want to reuse the same mountpoint for all unmounted shares to the same resource
		return existingMountpoint, unmountedShares, nil
	}

	// If the mount point really already exists, we need to insert a number into the filename
	if pathExists {
		// now we have a list of shares, we want to iterate over all of them and check for name collisions agents a mount points list
		for i := 1; i <= len(mountedShares)+1; i++ {
			ext := filepath.Ext(base)
			name := strings.TrimSuffix(base, ext)
			// be smart about .tar.(gz|bz) files
			if strings.HasSuffix(name, ".tar") {
				name = strings.TrimSuffix(name, ".tar")
				ext = ".tar" + ext
			}
			mount = name + " (" + strconv.Itoa(i) + ")" + ext
			if !slices.Contains(mountedShares, mount) {
				return mount, unmountedShares, nil
			}
		}
	}
	return mount, unmountedShares, nil
}

// listReceivedShares list all received shares for the current user.
func listReceivedShares(ctx context.Context, client gateway.GatewayAPIClient) ([]*collaboration.ReceivedShare, error) {
	res, err := client.ListReceivedShares(ctx, &collaboration.ListReceivedSharesRequest{})
	if err != nil {
		return nil, errtypes.InternalError("grpc list received shares request failed")
	}

	if err := errtypes.NewErrtypeFromStatus(res.Status); err != nil {
		return nil, err
	}
	return res.Shares, nil
}
