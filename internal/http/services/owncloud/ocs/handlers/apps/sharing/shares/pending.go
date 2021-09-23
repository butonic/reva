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

package shares

import (
	"fmt"
	"net/http"
	"path"
	"sort"
	"strconv"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	collaboration "github.com/cs3org/go-cs3apis/cs3/sharing/collaboration/v1beta1"
	"github.com/cs3org/reva/internal/http/services/owncloud/ocs/conversions"
	"github.com/cs3org/reva/internal/http/services/owncloud/ocs/response"
	"github.com/cs3org/reva/pkg/appctx"
	"github.com/cs3org/reva/pkg/rgrpc/todo/pool"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// AcceptReceivedShare handles Post Requests on /apps/files_sharing/api/v1/shares/{shareid}
func (h *Handler) AcceptReceivedShare(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	shareID := chi.URLParam(r, "shareid")
	// todo
	// 1. find free name to prevent collision
	// 2.

	client, err := pool.GetGatewayServiceClient(h.gatewayAddr)
	if err != nil {
		response.WriteOCSError(w, r, response.MetaServerError.StatusCode, "error getting grpc gateway client", err)
		return
	}

	// we need the original share in order to get where in the storage it is mounter to determine the name of
	// the shared resource.
	s, err := client.GetShare(ctx, &collaboration.GetShareRequest{
		Ref: &collaboration.ShareReference{
			Spec: &collaboration.ShareReference_Id{
				Id: &collaboration.ShareId{
					OpaqueId: shareID,
				}},
		},
	})
	if err != nil {
		response.WriteOCSError(w, r, response.MetaServerError.StatusCode,
			fmt.Sprintf("could not get share with ID: `%s`", shareID),
			err,
		)
		return
	}

	if s.Status.Code != rpc.Code_CODE_OK {
		if s.Status.Code == rpc.Code_CODE_NOT_FOUND {
			response.WriteOCSError(w, r, response.MetaNotFound.StatusCode, "not found", nil)
			return
		}
		response.WriteOCSError(w, r, response.MetaServerError.StatusCode, s.GetStatus().GetMessage(), nil)
		return
	}

	// get the name of the shared resource
	sharedResource, err := client.Stat(ctx, &provider.StatRequest{
		Ref: &provider.Reference{
			ResourceId: s.Share.GetResourceId(),
		},
	})
	if err != nil {
		response.WriteOCSError(w, r, response.MetaServerError.StatusCode, "could not get shared resource", err)
		return
	}

	if sharedResource.Status.Code != rpc.Code_CODE_OK {
		if sharedResource.Status.Code == rpc.Code_CODE_NOT_FOUND {
			response.WriteOCSError(w, r, response.MetaNotFound.StatusCode, "not found", nil)
			return
		}
		response.WriteOCSError(w, r, response.MetaServerError.StatusCode, sharedResource.GetStatus().GetMessage(), nil)
		return
	}

	// list received shares
	// TODO check if there is a span in the context.
	lrs, err := client.ListReceivedShares(r.Context(), &collaboration.ListReceivedSharesRequest{})
	if err != nil {
		response.WriteOCSError(w, r, response.MetaNotFound.StatusCode, "could not accept share", err)
		return
	}

	if lrs.Status.Code != rpc.Code_CODE_OK {
		if lrs.Status.Code == rpc.Code_CODE_NOT_FOUND {
			response.WriteOCSError(w, r, response.MetaNotFound.StatusCode, "not found", nil)
			return
		}
		response.WriteOCSError(w, r, response.MetaServerError.StatusCode, lrs.GetStatus().GetMessage(), nil)
		return
	}

	// we need to sort the received shares by mount point in order to make things easier to evaluate.
	mountPoints := []string{}
	for _, share := range lrs.Shares {
		if share.State == collaboration.ShareState_SHARE_STATE_ACCEPTED {
			// only when the share is accepted there is a mount point.
			mountPoints = append(mountPoints, share.MountPoint.Path)
		}
	}

	sort.Strings(mountPoints)
	base := path.Base(sharedResource.GetInfo().GetPath())
	mount := base

	// now we have a list of shares, we want to iterate over all of them and check for name collisions
	for i, mp := range mountPoints {
		if mp == mount {
			mount = fmt.Sprintf("%s (%s)", base, strconv.Itoa(i+1))
		}
	}

	h.updateReceivedShare(w, r, shareID, false, mount)
}

// RejectReceivedShare handles DELETE Requests on /apps/files_sharing/api/v1/shares/{shareid}
func (h *Handler) RejectReceivedShare(w http.ResponseWriter, r *http.Request) {
	shareID := chi.URLParam(r, "shareid")
	h.updateReceivedShare(w, r, shareID, true, "")
}

func (h *Handler) updateReceivedShare(w http.ResponseWriter, r *http.Request, shareID string, rejectShare bool, mountPoint string) {
	ctx := r.Context()
	logger := appctx.GetLogger(ctx)

	client, err := pool.GetGatewayServiceClient(h.gatewayAddr)
	if err != nil {
		response.WriteOCSError(w, r, response.MetaServerError.StatusCode, "error getting grpc gateway client", err)
		return
	}

	// we need to add a path to the share
	shareRequest := &collaboration.UpdateReceivedShareRequest{
		Share: &collaboration.ReceivedShare{
			Share: &collaboration.Share{Id: &collaboration.ShareId{OpaqueId: shareID}},
			MountPoint: &provider.Reference{
				Path: mountPoint,
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
	}
	if rejectShare {
		shareRequest.Share.State = collaboration.ShareState_SHARE_STATE_REJECTED
	} else {
		shareRequest.UpdateMask.Paths = append(shareRequest.UpdateMask.Paths, "mount_point")
		shareRequest.Share.State = collaboration.ShareState_SHARE_STATE_ACCEPTED
	}

	shareRes, err := client.UpdateReceivedShare(ctx, shareRequest)
	if err != nil {
		response.WriteOCSError(w, r, response.MetaServerError.StatusCode, "grpc update received share request failed", err)
		return
	}

	if shareRes.Status.Code != rpc.Code_CODE_OK {
		if shareRes.Status.Code == rpc.Code_CODE_NOT_FOUND {
			response.WriteOCSError(w, r, response.MetaNotFound.StatusCode, "not found", nil)
			return
		}
		response.WriteOCSError(w, r, response.MetaServerError.StatusCode, "grpc update received share request failed", errors.Errorf("code: %d, message: %s", shareRes.Status.Code, shareRes.Status.Message))
		return
	}

	rs := shareRes.GetShare()

	info, status, err := h.getResourceInfoByID(ctx, client, rs.Share.ResourceId)
	if err != nil || status.Code != rpc.Code_CODE_OK {
		h.logProblems(status, err, "could not stat, skipping")
	}

	data, err := conversions.CS3Share2ShareData(r.Context(), rs.Share)
	if err != nil {
		logger.Debug().Interface("share", rs.Share).Interface("shareData", data).Err(err).Msg("could not CS3Share2ShareData, skipping")
	}

	data.State = mapState(rs.GetState())

	if err := h.addFileInfo(ctx, data, info); err != nil {
		logger.Debug().Interface("received_share", rs).Interface("info", info).Interface("shareData", data).Err(err).Msg("could not add file info, skipping")
	}
	h.mapUserIds(r.Context(), client, data)

	if data.State == ocsStateAccepted {
		// Needed because received shares can be jailed in a folder in the users home
		data.Path = path.Join(h.sharePrefix, path.Base(info.Path))
	}

	response.WriteOCSSuccess(w, r, []*conversions.ShareData{data})
}
