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

package spaces

import (
	"context"
	"encoding/json"
	"time"

	providerpb "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	typesv1beta1 "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	"github.com/cs3org/reva/pkg/logger"
	"github.com/google/uuid"
)

type provider struct {
	Address string `mapstructure:"address"`
	Spaces  map[string]*providerpb.StorageSpace
	client  providerpb.ProviderAPIClient
}

// sleep is used to give the provider time to unsubscribe the registry and reset the stream
func (p *provider) sleep() {
	time.Sleep(time.Second * 5)
}

// Watch connects to a provider
func (p *provider) Watch() {
	log := logger.New()
	var err error
	var stream providerpb.ProviderAPI_ListContainerStreamClient
	clientID := uuid.New().String()
	for {
		if stream == nil {
			stream, err = p.client.ListContainerStream(context.Background(), &providerpb.ListContainerStreamRequest{
				Opaque: &typesv1beta1.Opaque{Map: map[string]*typesv1beta1.OpaqueEntry{
					"client_id": {
						Decoder: "plain",
						Value:   []byte(clientID),
					},
				}},
				// TODO should we disallow wildcards like this? we could send an opaque flag
				// IMO this captures the idea that we want to be notified of all spaces (well ... we could add filters?)
				Ref: &providerpb.Reference{ResourceId: &providerpb.ResourceId{StorageId: "*", OpaqueId: "*"}},
			})
			if err != nil {
				log.Debug().Err(err).Str("address", p.Address).Interface("client_id", clientID).Msg("failed to watch provider")
				p.sleep()
				// Retry on failure
				continue
			}
		}
		in, err := stream.Recv()
		if err != nil {
			log.Error().Err(err).Str("address", p.Address).Msg("failed receiving space")
			stream = nil
			p.sleep()
			continue
		}

		var space *providerpb.StorageSpace
		if space = spaceFromOpaque(in.Opaque); space == nil {
			log.Debug().Interface("msg", in).Msg("no space")
			continue
		}
		log.Debug().Str("address", p.Address).Interface("space", space).Msg("received space")
		// what about lost updates?
		// TODO put that into a map of space id to spaces?
	}
}

func spaceFromOpaque(o *typesv1beta1.Opaque) *providerpb.StorageSpace {
	if o == nil || o.Map == nil || o.Map["space"] == nil || o.Map["space"].Decoder != "json" {
		return nil
	}
	space := &providerpb.StorageSpace{}
	err := json.Unmarshal(o.Map["space"].Value, space)
	if err != nil {
		// TODO log
		return nil
	}
	return space
}
