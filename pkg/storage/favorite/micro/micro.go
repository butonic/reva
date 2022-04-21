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

package memory

import (
	"context"

	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v2/pkg/storage/favorite"
	"github.com/cs3org/reva/v2/pkg/storage/favorite/registry"
	"github.com/cs3org/reva/v2/pkg/utils/resourceid"
	"github.com/go-micro/plugins/v4/store/file"
	"github.com/go-micro/plugins/v4/store/memory"
	"github.com/go-micro/plugins/v4/store/redis"
	"go-micro.dev/v4/store"
)

func init() {
	registry.Register("micro", New)
}

const favoritesDB = "favorites"

type mgr struct {
	store store.Store
}

// New returns an instance of the micro based favorites manager.
func New(m map[string]interface{}) (favorite.Manager, error) {

	opts := []store.Option{
		store.Database(favoritesDB),
		// all operations use the user id as table
	}
	if nodes, ok := m["nodes"].([]string); ok {
		opts = append(opts, store.Nodes(nodes...))
	}

	var s store.Store
	switch m["plugin"] {
	case "memory":
		s = memory.NewStore(opts...)
	case "file":
		// TODO needs a DefaultDir option
		s = file.NewStore(opts...)
	case "redis":
		s = redis.NewStore(opts...)
	default:
		s = store.NewStore(opts...)
	}

	return &mgr{store: s}, nil
}

func (m *mgr) ListFavorites(_ context.Context, userID *user.UserId) ([]*provider.ResourceId, error) {
	keys, err := m.store.List(
		store.ListFrom(favoritesDB, userID.OpaqueId),
	)
	if err != nil {
		return nil, err
	}

	favorites := make([]*provider.ResourceId, 0, len(keys))
	for _, key := range keys {
		favorites = append(favorites, resourceid.OwnCloudResourceIDUnwrap(key))
	}
	return favorites, nil
}

func (m *mgr) SetFavorite(_ context.Context, userID *user.UserId, resourceInfo *provider.ResourceInfo) error {
	return m.store.Write(
		&store.Record{
			Key: resourceid.OwnCloudResourceIDWrap(resourceInfo.Id),
		},
		store.WriteTo(favoritesDB, userID.OpaqueId),
	)
}

func (m *mgr) UnsetFavorite(_ context.Context, userID *user.UserId, resourceInfo *provider.ResourceInfo) error {
	return m.store.Delete(
		resourceid.OwnCloudResourceIDWrap(resourceInfo.Id),
		store.DeleteFrom(favoritesDB, userID.OpaqueId),
	)
}
