// Copyright 2018-2020 CERN
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
	"net/http"
	"path"

	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	typespb "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"

	"github.com/cs3org/reva/pkg/appctx"
	tusd "github.com/tus/tusd/pkg/handler"
)

func (s *svc) handleTusPost(w http.ResponseWriter, r *http.Request, ns string) {
	ctx := r.Context()
	log := appctx.GetLogger(ctx)
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

	// read filename from metadata
	meta := tusd.ParseMetadataHeader(r.Header.Get("Upload-Metadata"))
	if meta["filename"] == "" {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}

	// append filename to current dir
	fn := path.Join(ns, r.URL.Path, meta["filename"])

	// check tus headers?

	// check if destination exists or is a file
	client, err := s.getClient()
	if err != nil {
		log.Error().Err(err).Msg("error getting grpc client")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	sReq := &provider.StatRequest{
		Ref: &provider.Reference{
			Spec: &provider.Reference_Path{Path: fn},
		},
	}
	sRes, err := client.Stat(ctx, sReq)
	if err != nil {
		log.Error().Err(err).Msg("error sending grpc stat request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if sRes.Status.Code != rpc.Code_CODE_OK {
		if sRes.Status.Code != rpc.Code_CODE_NOT_FOUND {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	info := sRes.Info
	if info != nil && info.Type != provider.ResourceType_RESOURCE_TYPE_FILE {
		log.Warn().Msg("resource is not a file")
		w.WriteHeader(http.StatusConflict)
		return
	}

	if info != nil {
		clientETag := r.Header.Get("If-Match")
		serverETag := info.Etag
		if clientETag != "" {
			if clientETag != serverETag {
				log.Warn().Str("client-etag", clientETag).Str("server-etag", serverETag).Msg("etags mismatch")
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
		}
	}

	// initiateUpload

	uReq := &provider.InitiateFileUploadRequest{
		Ref: &provider.Reference{
			Spec: &provider.Reference_Path{Path: fn},
		},
		Opaque: &typespb.Opaque{
			Map: map[string]*typespb.OpaqueEntry{
				"Upload-Length": {
					Decoder: "plain",
					Value:   []byte(r.Header.Get("Upload-Length")),
				},
			},
		},
	}

	uRes, err := client.InitiateFileUpload(ctx, uReq)
	if err != nil {
		log.Error().Err(err).Msg("error initiating file upload")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if uRes.Status.Code != rpc.Code_CODE_OK {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Location", uRes.UploadEndpoint)
	w.WriteHeader(http.StatusCreated)
}
