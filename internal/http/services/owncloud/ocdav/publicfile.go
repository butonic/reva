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
	//	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/pkg/appctx"
	"github.com/cs3org/reva/pkg/rhttp/router"
	"go.opencensus.io/trace"
	"net/http"
	"path"
	//	"strings"
)

// PublicFileHandler handles trashbin requests
type PublicFileHandler struct {
	namespace string
}

func (h *PublicFileHandler) init(ns string) error {
	h.namespace = path.Join("/", ns)
	return nil
}

// Handler handles requests
func (h *PublicFileHandler) Handler(s *svc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, relativePath := router.ShiftPath(r.URL.Path)

		if relativePath != "" {
			// accessing the file
			// TODO: fail if relativePath contains further slashes: 404
			// TODO: validate that the file name actuall matches
			switch r.Method {
			case "PROPFIND":
				s.handlePropfind(w, r, h.namespace)
			case http.MethodGet:
				s.handleGet(w, r, h.namespace)
			case http.MethodOptions:
				s.handleOptions(w, r, h.namespace)
			case http.MethodHead:
				s.handleHead(w, r, h.namespace)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		} else {
			// accessing the virtual parent folder
			switch r.Method {
			case "PROPFIND":
				s.handlePropfindOnToken(w, r, h.namespace)
			case http.MethodOptions:
				s.handleOptions(w, r, h.namespace)
			case http.MethodHead:
				s.handleHead(w, r, h.namespace)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		}
	})
}

// ns is the namespace that is prefixed to the path in the cs3 namespace
func (s *svc) handlePropfindOnToken(w http.ResponseWriter, r *http.Request, ns string) {
	ctx := r.Context()
	ctx, span := trace.StartSpan(ctx, "propfind")
	defer span.End()
	log := appctx.GetLogger(ctx)

	tokenStatInfo := ctx.Value("tokenStatInfo").(*provider.ResourceInfo)
	log.Debug().Interface("tokenStatInfo", tokenStatInfo).Msg("handlePropfindOnToken")

	depth := r.Header.Get("Depth")
	if depth == "" {
		depth = "1"
	}

	// see https://tools.ietf.org/html/rfc4918#section-10.2
	if depth != "0" && depth != "1" && depth != "infinity" {
		log.Error().Msgf("invalid Depth header value %s", depth)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	/*
		pf, status, err := readPropfind(r.Body)
		if err != nil {
			log.Error().Err(err).Msg("error reading propfind request")
			w.WriteHeader(status)
			return
		}

		client, err := s.getClient()
		if err != nil {
			log.Error().Err(err).Msg("error getting grpc client")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

			rootInfo := nil
			fileInfo := nil

			infos := []*provider.ResourceInfo{res.Info}

			if res.Info.Type == provider.ResourceType_RESOURCE_TYPE_CONTAINER && depth == "1" {
				req := &provider.ListContainerRequest{
					Ref: ref,
				}
				res, err := client.ListContainer(ctx, req)
				if err != nil {
					log.Error().Err(err).Msg("error sending list container grpc request")
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				if res.Status.Code != rpc.Code_CODE_OK {
					log.Err(err).Msg("error calling grpc list container")
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				infos = append(infos, res.Infos...)
			} else if res.Info.Type == provider.ResourceType_RESOURCE_TYPE_CONTAINER && depth == "infinity" {
				// FIXME: doesn't work cross-storage as the results will have the wrong paths!
				// use a stack to explore sub-containers breadth-first
				stack := []string{res.Info.Path}
				for len(stack) > 0 {
					// retrieve path on top of stack
					path := stack[len(stack)-1]
					ref = &provider.Reference{
						Spec: &provider.Reference_Path{Path: path},
					}
					req := &provider.ListContainerRequest{
						Ref: ref,
					}
					res, err := client.ListContainer(ctx, req)
					if err != nil {
						log.Error().Err(err).Str("path", path).Msg("error sending list container grpc request")
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					if res.Status.Code != rpc.Code_CODE_OK {
						log.Err(err).Str("path", path).Msg("error calling grpc list container")
						w.WriteHeader(http.StatusInternalServerError)
						return
					}

					infos = append(infos, res.Infos...)

					if depth != "infinity" {
						break
					}

					// TODO: stream response to avoid storing too many results in memory

					stack = stack[:len(stack)-1]

					// check sub-containers in reverse order and add them to the stack
					// the reversed order here will produce a more logical sorting of results
					for i := len(res.Infos) - 1; i >= 0; i-- {
						if res.Infos[i].Type == provider.ResourceType_RESOURCE_TYPE_CONTAINER {
							stack = append(stack, res.Infos[i].Path)
						}
					}
				}
			} else if publiclySharedFile && depth == "1" {
				infos = []*provider.ResourceInfo{}
				// if the request is to a public link, we need to add yet another value for the file entry.
				infos = append(infos, &provider.ResourceInfo{
					// append the shared as a container. Annex to OC10 standards.
					Type:  provider.ResourceType_RESOURCE_TYPE_CONTAINER,
					Mtime: res.Info.Mtime,
				})
				infos = append(infos, res.Info)
			} else if publiclySharedFile && depth == "0" {
				// this logic runs when uploading a file on a publicly shared folder.
				infos = []*provider.ResourceInfo{}
				infos = append(infos, res.Info)
			}

			propRes, err := s.formatPropfind(ctx, &pf, infos, ns, publiclySharedFile)
			if err != nil {
				log.Error().Err(err).Msg("error formatting propfind")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			w.Header().Set("DAV", "1, 3, extended-mkcol")
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			// let clients know this collection supports tus.io POST requests to start uploads
			if res.Info.Type == provider.ResourceType_RESOURCE_TYPE_CONTAINER && !s.c.DisableTus {
				w.Header().Add("Access-Control-Expose-Headers", "Tus-Resumable, Tus-Version, Tus-Extension")
				w.Header().Set("Tus-Resumable", "1.0.0")
				w.Header().Set("Tus-Version", "1.0.0")
				w.Header().Set("Tus-Extension", "creation,creation-with-upload")
			}
			w.WriteHeader(http.StatusMultiStatus)
			if _, err := w.Write([]byte(propRes)); err != nil {
				log.Err(err).Msg("error writing response")
			}
	*/
}
