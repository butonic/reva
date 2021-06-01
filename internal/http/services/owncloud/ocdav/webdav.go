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
	"net/http"
	"path"
)

// Common Webdav methods.
//
// Unless otherwise noted, these are defined in RFC 4918 section 9.
const (
	MethodPropfind  = "PROPFIND"
	MethodLock      = "LOCK"
	MethodUnlock    = "UNLOCK"
	MethodProppatch = "PROPPATCH"
	MethodMkcol     = "MKCOL"
	MethodMove      = "MOVE"
	MethodCopy      = "COPY"
	MethodReport    = "REPORT"
)

// WebDavHandler implements a dav endpoint
type WebDavHandler struct {
	namespace         string
	useLoggedInUserNS bool
}

func (h *WebDavHandler) init(ns string, useLoggedInUserNS bool) error {
	h.namespace = path.Join("/", ns)
	h.useLoggedInUserNS = useLoggedInUserNS
	return nil
}

// Handler handles requests
func (h *WebDavHandler) Handler(s *svc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ns := applyLayout(r.Context(), h.namespace, h.useLoggedInUserNS, r.URL.Path)
		switch r.Method {
		case MethodPropfind:
			s.handlePropfind(w, r, ns)
		case MethodLock:
			s.handleLock(w, r, ns)
		case MethodUnlock:
			s.handleUnlock(w, r, ns)
		case MethodProppatch:
			s.handleProppatch(w, r, ns)
		case MethodMkcol:
			s.handleMkcol(w, r, ns)
		case MethodMove:
			s.handleMove(w, r, ns)
		case MethodCopy:
			s.handleCopy(w, r, ns)
		case MethodReport:
			s.handleReport(w, r, ns)
		case http.MethodGet:
			s.handleGet(w, r, ns)
		case http.MethodPut:
			s.handlePut(w, r, ns)
		case http.MethodPost:
			s.handleTusPost(w, r, ns)
		case http.MethodOptions:
			s.handleOptions(w, r, ns)
		case http.MethodHead:
			s.handleHead(w, r, ns)
		case http.MethodDelete:
			s.handleDelete(w, r, ns)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}
