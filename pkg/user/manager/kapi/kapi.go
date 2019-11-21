// Copyright 2018-2019 CERN
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

package json

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/cs3org/reva/pkg/appctx"
	"github.com/cs3org/reva/pkg/logger"
	"github.com/cs3org/reva/pkg/token"
	"github.com/cs3org/reva/pkg/user"
	"github.com/cs3org/reva/pkg/user/manager/registry"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"

	typespb "github.com/cs3org/go-cs3apis/cs3/types"
	userproviderv0alphapb "github.com/cs3org/go-cs3apis/cs3/userprovider/v0alpha"
	"github.com/cs3org/reva/pkg/errtypes"
	"github.com/cs3org/reva/pkg/rhttp"
)

func init() {
	registry.Register("kapi", New)
}

type mgr struct {
	c        *config
	endpoint *url.URL
	iss      string
}

type config struct {
	Endpoint string `mapstructure:"endpoint"`
}

func parseConfig(m map[string]interface{}) (*config, error) {
	c := &config{
		Endpoint: "https://kopano.demo/api/gc/v1/",
	}
	if err := mapstructure.Decode(m, c); err != nil {
		err = errors.Wrap(err, "error decoding conf")
		return nil, err
	}
	return c, nil
}

// New returns a user manager implementation that reads a json file to provide user metadata.
func New(m map[string]interface{}) (user.Manager, error) {
	c, err := parseConfig(m)
	if err != nil {
		return nil, err
	}
	e, err := url.Parse(c.Endpoint)
	if err != nil {
		logger.New().Error().Err(err).
			Str("endpoint", c.Endpoint).
			Msg("could not parse endpoint")
	}
	return &mgr{
		c:        c,
		endpoint: e,
		iss:      e.Scheme + "//" + e.Hostname(), // TODO hardcoded ... no port
	}, nil
}

type gUsers struct {
	Value []gUser
}

type gUser struct {
	GivenName      string
	Mail           string
	Surname        string
	ID             string
	MobilePhone    string
	OfficeLocation string
	//@odata.context string
	JobTitle          string
	UserPrincipalName string
	DisplayName       string
}

func (m *mgr) GetUser(ctx context.Context, uid *typespb.UserId) (*userproviderv0alphapb.User, error) {
	tkn, ok := token.ContextGetToken(ctx)
	if !ok {
		return nil, errtypes.InvalidCredentials("token not found in context")
	}
	tgt := m.endpoint
	// TODO select only needed attributes
	tgt.Path = path.Join(tgt.Path, "users", uid.GetOpaqueId())
	httpReq, err := rhttp.NewRequest(ctx, "GET", tgt.String(), nil)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Authentication", "Bearer "+tkn)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	customHTTPClient := &http.Client{
		Transport: tr,
		Timeout:   time.Second * 10,
	}
	httpRes, getErr := customHTTPClient.Do(httpReq)
	if getErr != nil {
		return nil, getErr
	}
	// TODO check not found: return nil, errtypes.NotFound(uid.OpaqueId)

	body, readErr := ioutil.ReadAll(httpRes.Body)
	if readErr != nil {
		return nil, readErr
	}

	u := gUser{}
	jsonErr := json.Unmarshal(body, &u)
	if jsonErr != nil {
		return nil, jsonErr
	}
	return &userproviderv0alphapb.User{
		Id: &typespb.UserId{
			Idp:      m.iss,
			OpaqueId: u.ID,
		},
		Username:    u.UserPrincipalName,
		Mail:        u.Mail,
		DisplayName: u.DisplayName,
		// Groups:      []string{"sailing-lovers", "violin-haters", "physics-lovers"},
		// TODO put the rest into opaque data?
		MailVerified: true, // assumed true
	}, nil
}

func (m *mgr) FindUsers(ctx context.Context, query string) ([]*userproviderv0alphapb.User, error) {
	log := appctx.GetLogger(ctx)
	tkn, ok := token.ContextGetToken(ctx)
	if !ok {
		return nil, errtypes.InvalidCredentials("token not found in context")
	}
	tgt := m.endpoint
	// TODO select only needed attributes
	// TODO pagination
	tgt.Path = path.Join(tgt.Path, "users")
	httpReq, err := rhttp.NewRequest(ctx, "GET", tgt.String(), nil)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Authentication", "Bearer "+tkn)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	customHTTPClient := &http.Client{
		Transport: tr,
		Timeout:   time.Second * 10,
	}
	httpRes, getErr := customHTTPClient.Do(httpReq)
	if getErr != nil {
		return nil, getErr
	}
	// TODO check not found: return nil, errtypes.NotFound(uid.OpaqueId)

	body, readErr := ioutil.ReadAll(httpRes.Body)
	if readErr != nil {
		return nil, readErr
	}

	log.Debug().Str("body", string(body)).Msg("received")
	users := []*userproviderv0alphapb.User{}
	gus := gUsers{}
	jsonErr := json.Unmarshal(body, &gus)
	if jsonErr != nil {
		return nil, jsonErr
	}

	for i := range gus.Value {
		u := &userproviderv0alphapb.User{
			Id: &typespb.UserId{
				Idp:      m.iss,
				OpaqueId: gus.Value[i].ID,
			},
			Username:    gus.Value[i].UserPrincipalName,
			Mail:        gus.Value[i].Mail,
			DisplayName: gus.Value[i].DisplayName,
			// Groups:      []string{"sailing-lovers", "violin-haters", "physics-lovers"},
			// TODO put the rest into opaque data?
			MailVerified: true, // assumed true
		}
		users = append(users, u)
	}
	return users, nil
}

func (m *mgr) GetUserGroups(ctx context.Context, uid *typespb.UserId) ([]string, error) {
	user, err := m.GetUser(ctx, uid)
	if err != nil {
		return nil, err
	}
	return user.Groups, nil
}

func (m *mgr) IsInGroup(ctx context.Context, uid *typespb.UserId, group string) (bool, error) {
	user, err := m.GetUser(ctx, uid)
	if err != nil {
		return false, err
	}

	for _, g := range user.Groups {
		if group == g {
			return true, nil
		}
	}
	return false, nil
}
