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

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/pkg/errtypes"
)

func (fs *posixfs) ListRecycle(ctx context.Context) (items []*provider.RecycleItem, err error) {
	return nil, errtypes.NotSupported("ListRecycle")
}

func (fs *posixfs) RestoreRecycleItem(ctx context.Context, key string) (err error) {
	return errtypes.NotSupported("RestoreRecycleItem")

}

func (fs *posixfs) PurgeRecycleItem(ctx context.Context, key string) (err error) {
	return errtypes.NotSupported("PurgeRecycleItem")
}

func (fs *posixfs) EmptyRecycle(ctx context.Context) error {
	return errtypes.NotSupported("EmptyRecycle")
}
