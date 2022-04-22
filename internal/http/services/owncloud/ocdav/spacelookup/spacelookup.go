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

package spacelookup

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	gateway "github.com/cs3org/go-cs3apis/cs3/gateway/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	storageProvider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	typesv1beta1 "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	"github.com/cs3org/reva/v2/pkg/rgrpc/status"
	"github.com/cs3org/reva/v2/pkg/utils"
)

// LookupReferenceForPath returns:
// a reference with root and relative path
// the status and error for the lookup
func LookupReferenceForPath(ctx context.Context, client gateway.GatewayAPIClient, path string) (*storageProvider.Reference, *rpc.Status, error) {
	space, cs3Status, err := LookUpStorageSpaceForPath(ctx, client, path)
	if err != nil || cs3Status.Code != rpc.Code_CODE_OK {
		return nil, cs3Status, err
	}
	spacePath := string(space.Opaque.Map["path"].Value) // FIXME error checks
	return &storageProvider.Reference{
		ResourceId: space.Root,
		Path:       utils.MakeRelativePath(strings.TrimPrefix(path, spacePath)),
	}, cs3Status, nil
}

// LookUpStorageSpaceForPath returns:
// the storage spaces responsible for a path
// the status and error for the lookup
func LookUpStorageSpaceForPath(ctx context.Context, client gateway.GatewayAPIClient, path string) (*storageProvider.StorageSpace, *rpc.Status, error) {
	// TODO add filter to only fetch spaces changed in the last 30 sec?
	// TODO cache space information, invalidate after ... 5min? so we do not need to fetch all spaces?
	// TODO use ListContainerStream to listen for changes
	// retrieve a specific storage space
	lSSReq := &storageProvider.ListStorageSpacesRequest{
		Opaque: &typesv1beta1.Opaque{
			Map: map[string]*typesv1beta1.OpaqueEntry{
				"path": {
					Decoder: "plain",
					Value:   []byte(path),
				},
				"unique": {
					Decoder: "plain",
					Value:   []byte(strconv.FormatBool(true)),
				},
			},
		},
	}

	lSSRes, err := client.ListStorageSpaces(ctx, lSSReq)
	if err != nil || lSSRes.Status.Code != rpc.Code_CODE_OK {
		status := status.NewStatusFromErrType(ctx, "failed to lookup storage spaces", err)
		if lSSRes != nil {
			status = lSSRes.Status
		}
		return nil, status, err
	}
	switch len(lSSRes.StorageSpaces) {
	case 0:
		return nil, status.NewNotFound(ctx, "no space found"), nil
	case 1:
		return lSSRes.StorageSpaces[0], lSSRes.Status, nil
	}

	return nil, status.NewInternal(ctx, "too many spaces returned"), nil
}

// LookUpStorageSpacesForPathWithChildren returns:
// the list of storage spaces responsible for a path
// the status and error for the lookup
func LookUpStorageSpacesForPathWithChildren(ctx context.Context, client gateway.GatewayAPIClient, path string) ([]*storageProvider.StorageSpace, *rpc.Status, error) {
	// TODO add filter to only fetch spaces changed in the last 30 sec?
	// TODO cache space information, invalidate after ... 5min? so we do not need to fetch all spaces?
	// TODO use ListContainerStream to listen for changes
	// retrieve a specific storage space
	lSSReq := &storageProvider.ListStorageSpacesRequest{
		Opaque: &typesv1beta1.Opaque{
			Map: map[string]*typesv1beta1.OpaqueEntry{
				"path":            {Decoder: "plain", Value: []byte(path)},
				"withChildMounts": {Decoder: "plain", Value: []byte("true")},
			}},
	}

	lSSRes, err := client.ListStorageSpaces(ctx, lSSReq)
	if err != nil {
		return nil, nil, err
	}
	if lSSRes.Status.GetCode() != rpc.Code_CODE_OK {
		return nil, lSSRes.Status, err
	}

	return lSSRes.StorageSpaces, lSSRes.Status, nil
}

// LookUpStorageSpaceByID find a space by ID
func LookUpStorageSpaceByID(ctx context.Context, client gateway.GatewayAPIClient, spaceID string) (*storageProvider.StorageSpace, *rpc.Status, error) {
	// retrieve a specific storage space
	lSSReq := &storageProvider.ListStorageSpacesRequest{
		Opaque: &typesv1beta1.Opaque{},
		Filters: []*storageProvider.ListStorageSpacesRequest_Filter{
			{
				Type: storageProvider.ListStorageSpacesRequest_Filter_TYPE_ID,
				Term: &storageProvider.ListStorageSpacesRequest_Filter_Id{
					Id: &storageProvider.StorageSpaceId{
						OpaqueId: spaceID,
					},
				},
			},
		},
	}

	lSSRes, err := client.ListStorageSpaces(ctx, lSSReq)
	if err != nil || lSSRes.Status.Code != rpc.Code_CODE_OK {
		return nil, lSSRes.Status, err
	}

	switch len(lSSRes.StorageSpaces) {
	case 0:
		return nil, &rpc.Status{Code: rpc.Code_CODE_NOT_FOUND}, nil // since the caller only expects a single space return not found status
	case 1:
		return lSSRes.StorageSpaces[0], lSSRes.Status, nil
	default:
		return nil, nil, fmt.Errorf("unexpected number of spaces %d", len(lSSRes.StorageSpaces))
	}
}

// LookUpStorageSpaceReference find a space by id and returns a relative reference
func LookUpStorageSpaceReference(ctx context.Context, client gateway.GatewayAPIClient, spaceID string, relativePath string, spacesDavRequest bool) (*storageProvider.Reference, *rpc.Status, error) {
	space, status, err := LookUpStorageSpaceByID(ctx, client, spaceID)
	if space == nil {
		return nil, status, err
	}
	return MakeRelativeReference(space, relativePath, spacesDavRequest), status, err
}

// MakeRelativeReference returns a relative reference for the given space and path
func MakeRelativeReference(space *storageProvider.StorageSpace, relativePath string, spacesDavRequest bool) *storageProvider.Reference {
	spacePath := ""
	if space.Opaque != nil && space.Opaque.Map != nil && space.Opaque.Map["path"] != nil && space.Opaque.Map["path"].Decoder == "plain" {
		spacePath = string(space.Opaque.Map["path"].Value)
		// return nil // not mounted
	}
	relativeSpacePath := "."
	if strings.HasPrefix(relativePath, spacePath) {
		if space.Root == nil {
			// stay in path wonderland
			relativeSpacePath = relativePath
		} else {
			relativeSpacePath = utils.MakeRelativePath(strings.TrimPrefix(relativePath, spacePath))
		}
	} else if spacesDavRequest {
		relativeSpacePath = utils.MakeRelativePath(relativePath)
	}
	return &storageProvider.Reference{
		ResourceId: space.Root,
		Path:       relativeSpacePath,
	}
}
