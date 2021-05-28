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

package ocdav

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	gateway "github.com/cs3org/go-cs3apis/cs3/gateway/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	storageProvider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	typespb "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	"github.com/cs3org/reva/internal/http/services/datagateway"
	"github.com/cs3org/reva/pkg/appctx"
	"github.com/cs3org/reva/pkg/errtypes"
	"github.com/cs3org/reva/pkg/rhttp"
	"github.com/cs3org/reva/pkg/rhttp/router"
	"github.com/cs3org/reva/pkg/storage/utils/chunking"
	"github.com/cs3org/reva/pkg/utils"
	tusd "github.com/tus/tusd/pkg/handler"
	"go.opencensus.io/trace"
)

// SpacesHandler handles trashbin requests
type SpacesHandler struct {
	gatewaySvc string
}

func (h *SpacesHandler) init(c *Config) error {
	h.gatewaySvc = c.GatewaySvc
	return nil
}

// Handler handles requests
func (h *SpacesHandler) Handler(s *svc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// ctx := r.Context()
		// log := appctx.GetLogger(ctx)

		if r.Method == http.MethodOptions {
			s.handleOptions(w, r, "spaces")
			return
		}

		var spaceID string
		spaceID, r.URL.Path = router.ShiftPath(r.URL.Path)

		if spaceID == "" {
			// listing is disabled, no auth will change that
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		switch r.Method {
		case MethodPropfind:
			s.handleSpacesPropfind(w, r, spaceID)
		case MethodProppatch:
			s.handleSpacesProppatch(w, r, spaceID)
		case MethodLock:
			s.handleLock(w, r, spaceID)
		case MethodUnlock:
			s.handleUnlock(w, r, spaceID)
		case MethodMkcol:
			s.handleSpacesMkCol(w, r, spaceID)
		case MethodMove:
			s.handleSpacesMove(w, r, spaceID)
		case MethodCopy:
			s.handleSpacesCopy(w, r, spaceID)
		case MethodReport:
			s.handleReport(w, r, spaceID)
		case http.MethodGet:
			s.handleSpacesGet(w, r, spaceID)
		case http.MethodPut:
			s.handleSpacesPut(w, r, spaceID)
		case http.MethodPost:
			s.handleSpacesTusPost(w, r, spaceID)
		case http.MethodOptions:
			s.handleOptions(w, r, spaceID)
		case http.MethodHead:
			s.handleSpacesHead(w, r, spaceID)
		case http.MethodDelete:
			s.handleSpacesDelete(w, r, spaceID)
		default:
			http.Error(w, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
		}
	})
}

func (s *svc) lookUpStorageSpaceReference(ctx context.Context, spaceID string, relativePath string) (*storageProvider.Reference, *rpc.Status, error) {
	// Get the getway client
	gatewayClient, err := s.getClient()
	if err != nil {
		return nil, nil, err
	}

	// retrieve a specific storage space
	lSSReq := &storageProvider.ListStorageSpacesRequest{
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

	lSSRes, err := gatewayClient.ListStorageSpaces(ctx, lSSReq)
	if err != nil || lSSRes.Status.Code != rpc.Code_CODE_OK {
		return nil, lSSRes.Status, err
	}

	if len(lSSRes.StorageSpaces) != 1 {
		return nil, nil, fmt.Errorf("unexpected number of spaces")
	}
	space := lSSRes.StorageSpaces[0]

	// TODO:
	// Use ResourceId to make request to the actual storage provider via the gateway.
	// - Copy  the storageId from the storage space root
	// - set the opaque Id to /storageSpaceId/relativePath in
	// Correct fix would be to add a new Reference to the CS3API
	return &storageProvider.Reference{
		Spec: &storageProvider.Reference_Id{
			Id: &storageProvider.ResourceId{
				StorageId: space.Root.StorageId,
				OpaqueId:  filepath.Join("/", space.Root.OpaqueId, relativePath), // FIXME this is a hack to pass storage space id and a relative path to the storage provider
			},
		},
	}, lSSRes.Status, nil
}

func (s *svc) handleSpacesPropfind(w http.ResponseWriter, r *http.Request, spaceID string) {
	ctx := r.Context()
	ctx, span := trace.StartSpan(ctx, "propfind")
	defer span.End()

	depth := r.Header.Get("Depth")
	if depth == "" {
		depth = "1"
	}

	sublog := appctx.GetLogger(ctx).With().Str("path", r.URL.Path).Str("spaceid", spaceID).Logger()

	// see https://tools.ietf.org/html/rfc4918#section-9.1
	if depth != "0" && depth != "1" && depth != "infinity" {
		sublog.Debug().Str("depth", depth).Msgf("invalid Depth header value")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	pf, status, err := readPropfind(r.Body)
	if err != nil {
		sublog.Debug().Err(err).Msg("error reading propfind request")
		w.WriteHeader(status)
		return
	}

	// Get the getway client
	gatewayClient, err := s.getClient()
	if err != nil {
		sublog.Error().Err(err).Msg("error getting grpc client")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	metadataKeys := []string{}
	if pf.Allprop != nil {
		// TODO this changes the behavior and returns all properties if allprops has been set,
		// but allprops should only return some default properties
		// see https://tools.ietf.org/html/rfc4918#section-9.1
		// the description of arbitrary_metadata_keys in https://cs3org.github.io/cs3apis/#cs3.storage.provider.v1beta1.ListContainerRequest an others may need clarification
		// tracked in https://github.com/cs3org/cs3apis/issues/104
		metadataKeys = append(metadataKeys, "*")
	} else {
		for i := range pf.Prop {
			if requiresExplicitFetching(&pf.Prop[i]) {
				metadataKeys = append(metadataKeys, metadataKeyOf(&pf.Prop[i]))
			}
		}
	}

	// retrieve a specific storage space
	ref, rpcStatus, err := s.lookUpStorageSpaceReference(ctx, spaceID, r.URL.Path)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending a grpc request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if rpcStatus.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(&sublog, w, rpcStatus)
		return
	}

	req := &storageProvider.StatRequest{
		Ref:                   ref,
		ArbitraryMetadataKeys: metadataKeys,
	}
	res, err := gatewayClient.Stat(ctx, req)
	if err != nil {
		sublog.Error().Err(err).Interface("req", req).Msg("error sending a grpc stat request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if res.Status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(&sublog, w, res.Status)
		return
	}

	parentInfo := res.Info
	resourceInfos := []*storageProvider.ResourceInfo{parentInfo}
	if parentInfo.Type == storageProvider.ResourceType_RESOURCE_TYPE_CONTAINER && depth == "1" {
		req := &storageProvider.ListContainerRequest{
			Ref:                   ref,
			ArbitraryMetadataKeys: metadataKeys,
		}
		res, err := gatewayClient.ListContainer(ctx, req)
		if err != nil {
			sublog.Error().Err(err).Msg("error sending list container grpc request")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if res.Status.Code != rpc.Code_CODE_OK {
			HandleErrorStatus(&sublog, w, res.Status)
			return
		}
		resourceInfos = append(resourceInfos, res.Infos...)
	} else if depth == "infinity" {
		// FIXME: doesn't work cross-storage as the results will have the wrong paths!
		// use a stack to explore sub-containers breadth-first
		stack := []string{parentInfo.Path}
		for len(stack) > 0 {
			// retrieve path on top of stack
			currentPath := stack[len(stack)-1]
			ref = &storageProvider.Reference{
				Spec: &storageProvider.Reference_Path{Path: currentPath},
			}
			req := &storageProvider.ListContainerRequest{
				Ref:                   ref,
				ArbitraryMetadataKeys: metadataKeys,
			}
			res, err := gatewayClient.ListContainer(ctx, req)
			if err != nil {
				sublog.Error().Err(err).Str("path", currentPath).Msg("error sending list container grpc request")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if res.Status.Code != rpc.Code_CODE_OK {
				HandleErrorStatus(&sublog, w, res.Status)
				return
			}

			resourceInfos = append(resourceInfos, res.Infos...)

			if depth != "infinity" {
				break
			}

			// TODO: stream response to avoid storing too many results in memory

			stack = stack[:len(stack)-1]

			// check sub-containers in reverse order and add them to the stack
			// the reversed order here will produce a more logical sorting of results
			for i := len(res.Infos) - 1; i >= 0; i-- {
				// for i := range res.Infos {
				if res.Infos[i].Type == storageProvider.ResourceType_RESOURCE_TYPE_CONTAINER {
					stack = append(stack, res.Infos[i].Path)
				}
			}
		}
	}

	// prefix space id to paths
	for i := range resourceInfos {
		resourceInfos[i].Path = path.Join("/", spaceID, resourceInfos[i].Path)
	}

	propRes, err := s.formatPropfind(ctx, &pf, resourceInfos, "") // no namespace because this is relative to the storage space
	if err != nil {
		sublog.Error().Err(err).Msg("error formatting propfind")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("DAV", "1, 3, extended-mkcol")
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")

	var disableTus bool
	// let clients know this collection supports tus.io POST requests to start uploads
	if parentInfo.Type == storageProvider.ResourceType_RESOURCE_TYPE_CONTAINER {
		if parentInfo.Opaque != nil {
			_, disableTus = parentInfo.Opaque.Map["disable_tus"]
		}
		if !disableTus {
			w.Header().Add("Access-Control-Expose-Headers", "Tus-Resumable, Tus-Version, Tus-Extension")
			w.Header().Set("Tus-Resumable", "1.0.0")
			w.Header().Set("Tus-Version", "1.0.0")
			w.Header().Set("Tus-Extension", "creation,creation-with-upload")
		}
	}
	w.WriteHeader(http.StatusMultiStatus)
	if _, err := w.Write([]byte(propRes)); err != nil {
		sublog.Err(err).Msg("error writing response")
	}
}

func (s *svc) handleSpacesMkCol(w http.ResponseWriter, r *http.Request, spaceID string) {
	ctx := r.Context()
	ctx, span := trace.StartSpan(ctx, "mkcol")
	defer span.End()

	sublog := appctx.GetLogger(ctx).With().Str("path", r.URL.Path).Str("spaceid", spaceID).Str("handler", "mkcol").Logger()

	if r.Body != http.NoBody {
		w.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}

	ref, rpcStatus, err := s.lookUpStorageSpaceReference(ctx, spaceID, r.URL.Path)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending a grpc request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if rpcStatus.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(&sublog, w, rpcStatus)
		return
	}

	gatewayClient, err := s.getClient()
	if err != nil {
		sublog.Error().Err(err).Msg("error getting grpc client")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	statReq := &storageProvider.StatRequest{Ref: ref}
	statRes, err := gatewayClient.Stat(ctx, statReq)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending a grpc stat request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if statRes.Status.Code != rpc.Code_CODE_NOT_FOUND {
		if statRes.Status.Code == rpc.Code_CODE_OK {
			w.WriteHeader(http.StatusMethodNotAllowed) // 405 if it already exists
		} else {
			HandleErrorStatus(&sublog, w, statRes.Status)
		}
		return
	}

	req := &storageProvider.CreateContainerRequest{Ref: ref}
	res, err := gatewayClient.CreateContainer(ctx, req)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending create container grpc request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	switch res.Status.Code {
	case rpc.Code_CODE_OK:
		w.WriteHeader(http.StatusCreated)
	case rpc.Code_CODE_NOT_FOUND:
		sublog.Debug().Str("path", r.URL.Path).Interface("status", statRes.Status).Msg("conflict")
		w.WriteHeader(http.StatusConflict)
	default:
		HandleErrorStatus(&sublog, w, res.Status)
	}
}

func (s *svc) handleSpacesMove(w http.ResponseWriter, r *http.Request, srcSpaceID string) {
	ctx := r.Context()
	ctx, span := trace.StartSpan(ctx, "move")
	defer span.End()

	dstHeader := r.Header.Get("Destination")
	overwrite := r.Header.Get("Overwrite")

	dst, err := extractDestination(dstHeader, r.Context().Value(ctxKeyBaseURI).(string))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	sublog := appctx.GetLogger(ctx)
	sublog.Debug().Str("overwrite", overwrite).Msg("move")

	overwrite = strings.ToUpper(overwrite)
	if overwrite == "" {
		overwrite = "T"
	}

	if overwrite != "T" && overwrite != "F" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// retrieve a specific storage space
	srcRef, status, err := s.lookUpStorageSpaceReference(ctx, srcSpaceID, r.URL.Path)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending a grpc request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(sublog, w, status)
		return
	}

	client, err := s.getClient()
	if err != nil {
		sublog.Error().Err(err).Msg("error getting grpc client")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// check src exists
	srcStatReq := &storageProvider.StatRequest{Ref: srcRef}
	srcStatRes, err := client.Stat(ctx, srcStatReq)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending grpc stat request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if srcStatRes.Status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(sublog, w, srcStatRes.Status)
		return
	}

	dstSpaceID, dstRelPath := router.ShiftPath(dst)

	// retrieve a specific storage space
	dstRef, status, err := s.lookUpStorageSpaceReference(ctx, dstSpaceID, dstRelPath)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending a grpc request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(sublog, w, status)
		return
	}
	dstStatReq := &storageProvider.StatRequest{Ref: dstRef}
	dstStatRes, err := client.Stat(ctx, dstStatReq)
	if err != nil {
		sublog.Error().Err(err).Msg("error getting grpc client")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if dstStatRes.Status.Code != rpc.Code_CODE_OK && dstStatRes.Status.Code != rpc.Code_CODE_NOT_FOUND {
		HandleErrorStatus(sublog, w, srcStatRes.Status)
		return
	}

	successCode := http.StatusCreated // 201 if new resource was created, see https://tools.ietf.org/html/rfc4918#section-9.9.4

	if dstStatRes.Status.Code == rpc.Code_CODE_OK {
		successCode = http.StatusNoContent // 204 if target already existed, see https://tools.ietf.org/html/rfc4918#section-9.9.4

		if overwrite == "F" {
			sublog.Warn().Str("overwrite", overwrite).Msg("dst already exists")
			w.WriteHeader(http.StatusPreconditionFailed) // 412, see https://tools.ietf.org/html/rfc4918#section-9.9.4
			return
		}

		// delete existing tree
		delReq := &storageProvider.DeleteRequest{Ref: dstRef}
		delRes, err := client.Delete(ctx, delReq)
		if err != nil {
			sublog.Error().Err(err).Msg("error sending grpc delete request")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if delRes.Status.Code != rpc.Code_CODE_OK && delRes.Status.Code != rpc.Code_CODE_NOT_FOUND {
			HandleErrorStatus(sublog, w, delRes.Status)
			return
		}
	} else {
		// check if an intermediate path / the parent exists
		intermediateDir := path.Dir(dstRelPath)
		// retrieve a specific storage space
		dstRef, status, err := s.lookUpStorageSpaceReference(ctx, dstSpaceID, intermediateDir)
		if err != nil {
			sublog.Error().Err(err).Msg("error sending a grpc request")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if status.Code != rpc.Code_CODE_OK {
			HandleErrorStatus(sublog, w, status)
			return
		}
		intStatReq := &storageProvider.StatRequest{Ref: dstRef}
		intStatRes, err := client.Stat(ctx, intStatReq)
		if err != nil {
			sublog.Error().Err(err).Msg("error sending grpc stat request")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if intStatRes.Status.Code != rpc.Code_CODE_OK {
			if intStatRes.Status.Code == rpc.Code_CODE_NOT_FOUND {
				// 409 if intermediate dir is missing, see https://tools.ietf.org/html/rfc4918#section-9.8.5
				sublog.Debug().Str("parent", intermediateDir).Interface("status", intStatRes.Status).Msg("conflict")
				w.WriteHeader(http.StatusConflict)
			} else {
				HandleErrorStatus(sublog, w, intStatRes.Status)
			}
			return
		}
		// TODO what if intermediate is a file?
	}

	mReq := &storageProvider.MoveRequest{Source: srcRef, Destination: dstRef}
	mRes, err := client.Move(ctx, mReq)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending move grpc request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if mRes.Status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(sublog, w, mRes.Status)
		return
	}

	dstStatRes, err = client.Stat(ctx, dstStatReq)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending grpc stat request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if dstStatRes.Status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(sublog, w, dstStatRes.Status)
		return
	}

	info := dstStatRes.Info
	w.Header().Set("Content-Type", info.MimeType)
	w.Header().Set("ETag", info.Etag)
	w.Header().Set("OC-FileId", wrapResourceID(info.Id))
	w.Header().Set("OC-ETag", info.Etag)
	w.WriteHeader(successCode)
}

func (s *svc) handleSpacesProppatch(w http.ResponseWriter, r *http.Request, spaceID string) {
	ctx := r.Context()
	ctx, span := trace.StartSpan(ctx, "proppatch")
	defer span.End()

	sublog := appctx.GetLogger(ctx).With().Str("path", r.URL.Path).Str("spaceid", spaceID).Logger()

	pp, status, err := readProppatch(r.Body)
	if err != nil {
		sublog.Debug().Err(err).Msg("error reading proppatch")
		w.WriteHeader(status)
		return
	}

	c, err := s.getClient()
	if err != nil {
		sublog.Error().Err(err).Msg("error getting grpc client")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// retrieve a specific storage space
	ref, rpcStatus, err := s.lookUpStorageSpaceReference(ctx, spaceID, r.URL.Path)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending a grpc request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if rpcStatus.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(&sublog, w, rpcStatus)
		return
	}
	// check if resource exists
	statReq := &storageProvider.StatRequest{
		Ref: ref,
	}
	statRes, err := c.Stat(ctx, statReq)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending a grpc stat request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if statRes.Status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(&sublog, w, statRes.Status)
		return
	}

	rreq := &storageProvider.UnsetArbitraryMetadataRequest{
		Ref:                   ref,
		ArbitraryMetadataKeys: []string{""},
	}
	sreq := &storageProvider.SetArbitraryMetadataRequest{
		Ref: ref,
		ArbitraryMetadata: &storageProvider.ArbitraryMetadata{
			Metadata: map[string]string{},
		},
	}
	acceptedProps := []xml.Name{}
	removedProps := []xml.Name{}
	for i := range pp {
		if len(pp[i].Props) == 0 {
			continue
		}
		for j := range pp[i].Props {
			propNameXML := pp[i].Props[j].XMLName
			// don't use path.Join. It removes the double slash! concatenate with a /
			key := fmt.Sprintf("%s/%s", pp[i].Props[j].XMLName.Space, pp[i].Props[j].XMLName.Local)
			value := string(pp[i].Props[j].InnerXML)
			remove := pp[i].Remove
			// boolean flags may be "set" to false as well
			if s.isBooleanProperty(key) {
				// Make boolean properties either "0" or "1"
				value = s.as0or1(value)
				if value == "0" {
					remove = true
				}
			}
			// Webdav spec requires the operations to be executed in the order
			// specified in the PROPPATCH request
			// http://www.webdav.org/specs/rfc2518.html#rfc.section.8.2
			// FIXME: batch this somehow
			if remove {
				rreq.ArbitraryMetadataKeys[0] = key
				res, err := c.UnsetArbitraryMetadata(ctx, rreq)
				if err != nil {
					sublog.Error().Err(err).Msg("error sending a grpc UnsetArbitraryMetadata request")
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				if res.Status.Code != rpc.Code_CODE_OK {
					HandleErrorStatus(&sublog, w, res.Status)
					return
				}
				removedProps = append(removedProps, propNameXML)
			} else {
				sreq.ArbitraryMetadata.Metadata[key] = value
				res, err := c.SetArbitraryMetadata(ctx, sreq)
				if err != nil {
					sublog.Error().Err(err).Str("key", key).Str("value", value).Msg("error sending a grpc SetArbitraryMetadata request")
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				if res.Status.Code != rpc.Code_CODE_OK {
					HandleErrorStatus(&sublog, w, res.Status)
					return
				}

				acceptedProps = append(acceptedProps, propNameXML)
				delete(sreq.ArbitraryMetadata.Metadata, key)
			}
		}
		// FIXME: in case of error, need to set all properties back to the original state,
		// and return the error in the matching propstat block, if applicable
		// http://www.webdav.org/specs/rfc2518.html#rfc.section.8.2
	}

	// nRef := strings.TrimPrefix(fn, ns)
	nRef := path.Join(spaceID, statRes.Info.Path)
	nRef = path.Join(ctx.Value(ctxKeyBaseURI).(string), nRef)
	if statRes.Info.Type == storageProvider.ResourceType_RESOURCE_TYPE_CONTAINER {
		nRef += "/"
	}

	propRes, err := s.formatProppatchResponse(ctx, acceptedProps, removedProps, nRef)
	if err != nil {
		sublog.Error().Err(err).Msg("error formatting proppatch response")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("DAV", "1, 3, extended-mkcol")
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	if _, err := w.Write([]byte(propRes)); err != nil {
		sublog.Err(err).Msg("error writing response")
	}
}

func (s *svc) handleSpacesCopy(w http.ResponseWriter, r *http.Request, spaceID string) {
	ctx := r.Context()
	ctx, span := trace.StartSpan(ctx, "head")
	defer span.End()

	dstHeader := r.Header.Get("Destination")
	overwrite := r.Header.Get("Overwrite")
	depth := r.Header.Get("Depth")
	if depth == "" {
		depth = "infinity"
	}

	dst, err := extractDestination(dstHeader, r.Context().Value(ctxKeyBaseURI).(string))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	sublog := appctx.GetLogger(ctx).With().Str("spaceid", spaceID).Str("path", r.URL.Path).Logger()
	sublog.Debug().Str("overwrite", overwrite).Str("depth", depth).Msg("copy")

	overwrite = strings.ToUpper(overwrite)
	if overwrite == "" {
		overwrite = "T"
	}

	if overwrite != "T" && overwrite != "F" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if depth != "infinity" && depth != "0" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// retrieve a specific storage space
	srcRef, status, err := s.lookUpStorageSpaceReference(ctx, spaceID, r.URL.Path)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending a grpc request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(&sublog, w, status)
		return
	}

	client, err := s.getClient()
	if err != nil {
		sublog.Error().Err(err).Msg("error getting grpc client")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	srcStatReq := &storageProvider.StatRequest{Ref: srcRef}
	srcStatRes, err := client.Stat(ctx, srcStatReq)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending grpc stat request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if srcStatRes.Status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(&sublog, w, srcStatRes.Status)
		return
	}

	dstSpaceID, dstRelPath := router.ShiftPath(dst)

	// retrieve a specific storage space
	dstRef, status, err := s.lookUpStorageSpaceReference(ctx, dstSpaceID, dstRelPath)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending a grpc request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(&sublog, w, status)
		return
	}
	// check dst exists
	dstStatReq := &storageProvider.StatRequest{Ref: dstRef}
	dstStatRes, err := client.Stat(ctx, dstStatReq)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending grpc stat request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if dstStatRes.Status.Code != rpc.Code_CODE_OK && dstStatRes.Status.Code != rpc.Code_CODE_NOT_FOUND {
		HandleErrorStatus(&sublog, w, srcStatRes.Status)
		return
	}

	successCode := http.StatusCreated // 201 if new resource was created, see https://tools.ietf.org/html/rfc4918#section-9.8.5
	if dstStatRes.Status.Code == rpc.Code_CODE_OK {
		successCode = http.StatusNoContent // 204 if target already existed, see https://tools.ietf.org/html/rfc4918#section-9.8.5

		if overwrite == "F" {
			sublog.Warn().Str("overwrite", overwrite).Msg("dst already exists")
			w.WriteHeader(http.StatusPreconditionFailed) // 412, see https://tools.ietf.org/html/rfc4918#section-9.8.5
			return
		}

	} else {
		// check if an intermediate path / the parent exists
		intermediateDir := path.Dir(dstRelPath)
		// retrieve a specific storage space
		intermediateRef, status, err := s.lookUpStorageSpaceReference(ctx, dstSpaceID, intermediateDir)
		if err != nil {
			sublog.Error().Err(err).Msg("error sending a grpc request")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if status.Code != rpc.Code_CODE_OK {
			HandleErrorStatus(&sublog, w, status)
			return
		}
		intStatReq := &storageProvider.StatRequest{Ref: intermediateRef}
		intStatRes, err := client.Stat(ctx, intStatReq)
		if err != nil {
			sublog.Error().Err(err).Msg("error sending grpc stat request")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if intStatRes.Status.Code != rpc.Code_CODE_OK {
			if intStatRes.Status.Code == rpc.Code_CODE_NOT_FOUND {
				// 409 if intermediate dir is missing, see https://tools.ietf.org/html/rfc4918#section-9.8.5
				sublog.Debug().Str("parent", intermediateDir).Interface("status", intStatRes.Status).Msg("conflict")
				w.WriteHeader(http.StatusConflict)
			} else {
				HandleErrorStatus(&sublog, w, srcStatRes.Status)
			}
			return
		}
		// TODO what if intermediate is a file?
	}

	err = s.descendSpaces(ctx, client, srcStatRes.Info, dstRef, depth == "infinity")
	if err != nil {
		sublog.Error().Err(err).Str("depth", depth).Msg("error descending directory")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(successCode)
}
func (s *svc) descendSpaces(ctx context.Context, client gateway.GatewayAPIClient, src *storageProvider.ResourceInfo, dst *storageProvider.Reference, recurse bool) error {
	log := appctx.GetLogger(ctx)
	log.Debug().Str("src", src.Path).Interface("dst", dst).Msg("descending")
	if src.Type == storageProvider.ResourceType_RESOURCE_TYPE_CONTAINER {
		// create dir
		createReq := &storageProvider.CreateContainerRequest{
			Ref: dst,
		}
		createRes, err := client.CreateContainer(ctx, createReq)
		if err != nil || createRes.Status.Code != rpc.Code_CODE_OK {
			return err
		}

		// TODO: also copy properties: https://tools.ietf.org/html/rfc4918#section-9.8.2

		if !recurse {
			return nil
		}

		spaceID, _ := router.ShiftPath(dst.GetId().OpaqueId)

		// descend for children
		listReq := &storageProvider.ListContainerRequest{
			Ref: &storageProvider.Reference{
				Spec: &storageProvider.Reference_Id{
					Id: &storageProvider.ResourceId{
						StorageId: dst.GetId().StorageId,
						OpaqueId:  path.Join("/", spaceID, src.Path),
					}},
			},
		}
		res, err := client.ListContainer(ctx, listReq)
		if err != nil {
			return err
		}
		if res.Status.Code != rpc.Code_CODE_OK {
			return fmt.Errorf("status code %d", res.Status.Code)
		}

		for i := range res.Infos {
			// childDst := path.Join(dst, path.Base(res.Infos[i].Path))
			childRef := &storageProvider.Reference{
				Spec: &storageProvider.Reference_Id{
					Id: &storageProvider.ResourceId{
						StorageId: dst.GetId().StorageId,
						OpaqueId:  path.Join(dst.GetId().OpaqueId, "..", res.Infos[i].Path),
					},
				},
			}
			err := s.descendSpaces(ctx, client, res.Infos[i], childRef, recurse)
			if err != nil {
				return err
			}
		}

	} else {
		// copy file

		// 1. get download url

		spaceID, _ := router.ShiftPath(dst.GetId().OpaqueId)
		dReq := &storageProvider.InitiateFileDownloadRequest{
			Ref: &storageProvider.Reference{
				Spec: &storageProvider.Reference_Id{
					Id: &storageProvider.ResourceId{
						StorageId: dst.GetId().StorageId,
						OpaqueId:  path.Join("/", spaceID, src.Path),
					},
				},
			},
		}

		dRes, err := client.InitiateFileDownload(ctx, dReq)
		if err != nil {
			return err
		}

		if dRes.Status.Code != rpc.Code_CODE_OK {
			return fmt.Errorf("status code %d", dRes.Status.Code)
		}

		var downloadEP, downloadToken string
		for _, p := range dRes.Protocols {
			if p.Protocol == "spaces" {
				downloadEP, downloadToken = p.DownloadEndpoint, p.Token
			}
		}

		// 2. get upload url

		uReq := &storageProvider.InitiateFileUploadRequest{
			Ref: dst,
			Opaque: &typespb.Opaque{
				Map: map[string]*typespb.OpaqueEntry{
					"Upload-Length": {
						Decoder: "plain",
						// TODO: handle case where size is not known in advance
						Value: []byte(strconv.FormatUint(src.GetSize(), 10)),
					},
				},
			},
		}

		uRes, err := client.InitiateFileUpload(ctx, uReq)
		if err != nil {
			return err
		}

		if uRes.Status.Code != rpc.Code_CODE_OK {
			return fmt.Errorf("status code %d", uRes.Status.Code)
		}

		var uploadEP, uploadToken string
		for _, p := range uRes.Protocols {
			if p.Protocol == "simple" {
				uploadEP, uploadToken = p.UploadEndpoint, p.Token
			}
		}

		// 3. do download

		httpDownloadReq, err := rhttp.NewRequest(ctx, "GET", downloadEP, nil)
		if err != nil {
			return err
		}
		httpDownloadReq.Header.Set(datagateway.TokenTransportHeader, downloadToken)

		httpDownloadRes, err := s.client.Do(httpDownloadReq)
		if err != nil {
			return err
		}
		defer httpDownloadRes.Body.Close()
		if httpDownloadRes.StatusCode != http.StatusOK {
			return fmt.Errorf("status code %d", httpDownloadRes.StatusCode)
		}

		// 4. do upload

		if src.GetSize() > 0 {
			httpUploadReq, err := rhttp.NewRequest(ctx, "PUT", uploadEP, httpDownloadRes.Body)
			if err != nil {
				return err
			}
			httpUploadReq.Header.Set(datagateway.TokenTransportHeader, uploadToken)

			httpUploadRes, err := s.client.Do(httpUploadReq)
			if err != nil {
				return err
			}
			defer httpUploadRes.Body.Close()
			if httpUploadRes.StatusCode != http.StatusOK {
				return err
			}
		}
	}
	return nil
}

func (s *svc) handleSpacesPut(w http.ResponseWriter, r *http.Request, spaceID string) {
	ctx := r.Context()

	sublog := appctx.GetLogger(ctx).With().Str("spaceid", spaceID).Str("path", r.URL.Path).Logger()

	if r.Body == nil {
		sublog.Debug().Msg("body is nil")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if isContentRange(r) {
		sublog.Debug().Msg("Content-Range not supported for PUT")
		w.WriteHeader(http.StatusNotImplemented)
		return
	}

	if sufferMacOSFinder(r) {
		err := handleMacOSFinder(w, r)
		if err != nil {
			sublog.Debug().Err(err).Msg("error handling Mac OS corner-case")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	length, err := strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		// Fallback to Upload-Length
		length, err = strconv.ParseInt(r.Header.Get("Upload-Length"), 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	spaceRef, status, err := s.lookUpStorageSpaceReference(ctx, spaceID, r.URL.Path)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending a grpc request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(&sublog, w, status)
		return
	}

	s.handleSpacesPutHelper(w, r, r.Body, spaceRef, length)
}

func (s *svc) handleSpacesPutHelper(w http.ResponseWriter, r *http.Request, content io.Reader, ref *storageProvider.Reference, length int64) {
	ctx := r.Context()
	ctx, span := trace.StartSpan(ctx, "put")
	defer span.End()

	sublog := appctx.GetLogger(ctx)
	client, err := s.getClient()
	if err != nil {
		sublog.Error().Err(err).Msg("error getting grpc client")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	sReq := &storageProvider.StatRequest{Ref: ref}
	sRes, err := client.Stat(ctx, sReq)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending grpc stat request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if sRes.Status.Code != rpc.Code_CODE_OK && sRes.Status.Code != rpc.Code_CODE_NOT_FOUND {
		HandleErrorStatus(sublog, w, sRes.Status)
		return
	}

	info := sRes.Info
	if info != nil {
		if info.Type != storageProvider.ResourceType_RESOURCE_TYPE_FILE {
			sublog.Debug().Msg("resource is not a file")
			w.WriteHeader(http.StatusConflict)
			return
		}
		clientETag := r.Header.Get("If-Match")
		serverETag := info.Etag
		if clientETag != "" {
			if clientETag != serverETag {
				sublog.Debug().Str("client-etag", clientETag).Str("server-etag", serverETag).Msg("etags mismatch")
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
		}
	}

	opaqueMap := map[string]*typespb.OpaqueEntry{
		"Upload-Length": {
			Decoder: "plain",
			Value:   []byte(strconv.FormatInt(length, 10)),
		},
	}

	if mtime := r.Header.Get("X-OC-Mtime"); mtime != "" {
		opaqueMap["X-OC-Mtime"] = &typespb.OpaqueEntry{
			Decoder: "plain",
			Value:   []byte(mtime),
		}

		// TODO: find a way to check if the storage really accepted the value
		w.Header().Set("X-OC-Mtime", "accepted")
	}

	// curl -X PUT https://demo.owncloud.com/remote.php/webdav/testcs.bin -u demo:demo -d '123' -v -H 'OC-Checksum: SHA1:40bd001563085fc35165329ea1ff5c5ecbdbbeef'

	var cparts []string
	// TUS Upload-Checksum header takes precedence
	if checksum := r.Header.Get("Upload-Checksum"); checksum != "" {
		cparts = strings.SplitN(checksum, " ", 2)
		if len(cparts) != 2 {
			sublog.Debug().Str("upload-checksum", checksum).Msg("invalid Upload-Checksum format, expected '[algorithm] [checksum]'")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Then try owncloud header
	} else if checksum := r.Header.Get("OC-Checksum"); checksum != "" {
		cparts = strings.SplitN(checksum, ":", 2)
		if len(cparts) != 2 {
			sublog.Debug().Str("oc-checksum", checksum).Msg("invalid OC-Checksum format, expected '[algorithm]:[checksum]'")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
	// we do not check the algorithm here, because it might depend on the storage
	if len(cparts) == 2 {
		// Translate into TUS style Upload-Checksum header
		opaqueMap["Upload-Checksum"] = &typespb.OpaqueEntry{
			Decoder: "plain",
			// algorithm is always lowercase, checksum is separated by space
			Value: []byte(strings.ToLower(cparts[0]) + " " + cparts[1]),
		}
	}

	uReq := &storageProvider.InitiateFileUploadRequest{
		Ref:    ref,
		Opaque: &typespb.Opaque{Map: opaqueMap},
	}

	// where to upload the file?
	uRes, err := client.InitiateFileUpload(ctx, uReq)
	if err != nil {
		sublog.Error().Err(err).Msg("error initiating file upload")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if uRes.Status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(sublog, w, uRes.Status)
		return
	}

	var ep, token string
	for _, p := range uRes.Protocols {
		if p.Protocol == "simple" {
			ep, token = p.UploadEndpoint, p.Token
		}
	}

	if length > 0 {
		httpReq, err := rhttp.NewRequest(ctx, "PUT", ep, content)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		httpReq.Header.Set(datagateway.TokenTransportHeader, token)

		httpRes, err := s.client.Do(httpReq)
		if err != nil {
			sublog.Error().Err(err).Msg("error doing PUT request to data service")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer httpRes.Body.Close()
		if httpRes.StatusCode != http.StatusOK {
			if httpRes.StatusCode == http.StatusPartialContent {
				w.WriteHeader(http.StatusPartialContent)
				return
			}
			if httpRes.StatusCode == errtypes.StatusChecksumMismatch {
				w.WriteHeader(http.StatusBadRequest)
				b, err := Marshal(exception{
					code:    SabredavMethodBadRequest,
					message: "The computed checksum does not match the one received from the client.",
				})
				if err != nil {
					sublog.Error().Msgf("error marshaling xml response: %s", b)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				_, err = w.Write(b)
				if err != nil {
					sublog.Err(err).Msg("error writing response")
				}
				return
			}
			sublog.Error().Err(err).Msg("PUT request to data server failed")
			w.WriteHeader(httpRes.StatusCode)
			return
		}
	}

	ok, err := chunking.IsChunked(ref.GetId().GetOpaqueId())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if ok {
		chunk, err := chunking.GetChunkBLOBInfo(ref.GetId().GetOpaqueId())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		sReq = &storageProvider.StatRequest{
			Ref: &storageProvider.Reference{
				Spec: &storageProvider.Reference_Path{
					Path: chunk.Path,
				},
			},
		}
	}

	// stat again to check the new file's metadata
	sRes, err = client.Stat(ctx, sReq)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending grpc stat request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if sRes.Status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(sublog, w, sRes.Status)
		return
	}

	newInfo := sRes.Info

	w.Header().Add("Content-Type", newInfo.MimeType)
	w.Header().Set("ETag", newInfo.Etag)
	w.Header().Set("OC-FileId", wrapResourceID(newInfo.Id))
	w.Header().Set("OC-ETag", newInfo.Etag)
	t := utils.TSToTime(newInfo.Mtime).UTC()
	lastModifiedString := t.Format(time.RFC1123Z)
	w.Header().Set("Last-Modified", lastModifiedString)

	// file was new
	if info == nil {
		w.WriteHeader(http.StatusCreated)
		return
	}

	// overwrite
	w.WriteHeader(http.StatusNoContent)
}

func (s *svc) handleSpacesTusPost(w http.ResponseWriter, r *http.Request, spaceID string) {
	ctx := r.Context()
	ctx, span := trace.StartSpan(ctx, "tus-post")
	defer span.End()

	w.Header().Add("Access-Control-Allow-Headers", "Tus-Resumable, Upload-Length, Upload-Metadata, If-Match")
	w.Header().Add("Access-Control-Expose-Headers", "Tus-Resumable, Location")

	w.Header().Set("Tus-Resumable", "1.0.0")

	// Test if the version sent by the client is supported
	// GET methods are not checked since a browser may visit this URL and does
	// not include this header. This request is not part of the specification.
	if r.Header.Get("Tus-Resumable") != "1.0.0" {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	if r.Header.Get("Upload-Length") == "" {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	// r.Header.Get("OC-Checksum")
	// TODO must be SHA1, ADLER32 or MD5 ... in capital letters????
	// curl -X PUT https://demo.owncloud.com/remote.php/webdav/testcs.bin -u demo:demo -d '123' -v -H 'OC-Checksum: SHA1:40bd001563085fc35165329ea1ff5c5ecbdbbeef'

	// TODO check Expect: 100-continue

	// read filename from metadata
	meta := tusd.ParseMetadataHeader(r.Header.Get("Upload-Metadata"))
	if meta["filename"] == "" {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}

	// append filename to current dir
	// fn := path.Join(ns, r.URL.Path, meta["filename"])

	sublog := appctx.GetLogger(ctx).With().Str("spaceid", spaceID).Str("path", r.URL.Path).Logger()
	// check tus headers?

	// check if destination exists or is a file
	client, err := s.getClient()
	if err != nil {
		sublog.Error().Err(err).Msg("error getting grpc client")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	spaceRef, status, err := s.lookUpStorageSpaceReference(ctx, spaceID, r.URL.Path)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending a grpc request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(&sublog, w, status)
		return
	}

	sReq := &storageProvider.StatRequest{
		Ref: spaceRef,
	}
	sRes, err := client.Stat(ctx, sReq)
	if err != nil {
		sublog.Error().Err(err).Msg("error sending grpc stat request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if sRes.Status.Code != rpc.Code_CODE_OK && sRes.Status.Code != rpc.Code_CODE_NOT_FOUND {
		HandleErrorStatus(&sublog, w, sRes.Status)
		return
	}

	info := sRes.Info
	if info != nil && info.Type != storageProvider.ResourceType_RESOURCE_TYPE_FILE {
		sublog.Warn().Msg("resource is not a file")
		w.WriteHeader(http.StatusConflict)
		return
	}

	if info != nil {
		clientETag := r.Header.Get("If-Match")
		serverETag := info.Etag
		if clientETag != "" {
			if clientETag != serverETag {
				sublog.Warn().Str("client-etag", clientETag).Str("server-etag", serverETag).Msg("etags mismatch")
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
		}
	}

	opaqueMap := map[string]*typespb.OpaqueEntry{
		"Upload-Length": {
			Decoder: "plain",
			Value:   []byte(r.Header.Get("Upload-Length")),
		},
	}

	mtime := meta["mtime"]
	if mtime != "" {
		opaqueMap["X-OC-Mtime"] = &typespb.OpaqueEntry{
			Decoder: "plain",
			Value:   []byte(mtime),
		}
	}

	// initiateUpload
	uReq := &storageProvider.InitiateFileUploadRequest{
		Ref: spaceRef,
		Opaque: &typespb.Opaque{
			Map: opaqueMap,
		},
	}

	uRes, err := client.InitiateFileUpload(ctx, uReq)
	if err != nil {
		sublog.Error().Err(err).Msg("error initiating file upload")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if uRes.Status.Code != rpc.Code_CODE_OK {
		HandleErrorStatus(&sublog, w, uRes.Status)
		return
	}

	var ep, token string
	for _, p := range uRes.Protocols {
		if p.Protocol == "tus" {
			ep, token = p.UploadEndpoint, p.Token
		}
	}

	// TUS clients don't understand the reva transfer token. We need to append it to the upload endpoint.
	// The DataGateway has to take care of pulling it back into the request header upon request arrival.
	if token != "" {
		if !strings.HasSuffix(ep, "/") {
			ep += "/"
		}
		ep += token
	}

	w.Header().Set("Location", ep)

	// for creation-with-upload extension forward bytes to dataprovider
	// TODO check this really streams
	if r.Header.Get("Content-Type") == "application/offset+octet-stream" {

		length, err := strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64)
		if err != nil {
			sublog.Debug().Err(err).Msg("wrong request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var httpRes *http.Response

		if length != 0 {
			httpReq, err := rhttp.NewRequest(ctx, "PATCH", ep, r.Body)
			if err != nil {
				sublog.Debug().Err(err).Msg("wrong request")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			httpReq.Header.Set("Content-Type", r.Header.Get("Content-Type"))
			httpReq.Header.Set("Content-Length", r.Header.Get("Content-Length"))
			if r.Header.Get("Upload-Offset") != "" {
				httpReq.Header.Set("Upload-Offset", r.Header.Get("Upload-Offset"))
			} else {
				httpReq.Header.Set("Upload-Offset", "0")
			}
			httpReq.Header.Set("Tus-Resumable", r.Header.Get("Tus-Resumable"))

			httpRes, err = s.client.Do(httpReq)
			if err != nil {
				sublog.Error().Err(err).Msg("error doing GET request to data service")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer httpRes.Body.Close()

			w.Header().Set("Upload-Offset", httpRes.Header.Get("Upload-Offset"))
			w.Header().Set("Tus-Resumable", httpRes.Header.Get("Tus-Resumable"))
			if httpRes.StatusCode != http.StatusNoContent {
				w.WriteHeader(httpRes.StatusCode)
				return
			}
		} else {
			sublog.Debug().Msg("Skipping sending a Patch request as body is empty")
		}

		// check if upload was fully completed
		if length == 0 || httpRes.Header.Get("Upload-Offset") == r.Header.Get("Upload-Length") {
			// get uploaded file metadata
			sRes, err := client.Stat(ctx, sReq)
			if err != nil {
				sublog.Error().Err(err).Msg("error sending grpc stat request")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			if sRes.Status.Code != rpc.Code_CODE_OK && sRes.Status.Code != rpc.Code_CODE_NOT_FOUND {
				HandleErrorStatus(&sublog, w, sRes.Status)
				return
			}

			info := sRes.Info
			if info == nil {
				sublog.Error().Msg("No info found for uploaded file")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if httpRes != nil && httpRes.Header != nil && httpRes.Header.Get("X-OC-Mtime") != "" {
				// set the "accepted" value if returned in the upload response headers
				w.Header().Set("X-OC-Mtime", httpRes.Header.Get("X-OC-Mtime"))
			}

			w.Header().Set("Content-Type", info.MimeType)
			w.Header().Set("OC-FileId", wrapResourceID(info.Id))
			w.Header().Set("OC-ETag", info.Etag)
			w.Header().Set("ETag", info.Etag)
			t := utils.TSToTime(info.Mtime).UTC()
			lastModifiedString := t.Format(time.RFC1123Z)
			w.Header().Set("Last-Modified", lastModifiedString)
		}
	}

	w.WriteHeader(http.StatusCreated)
}
