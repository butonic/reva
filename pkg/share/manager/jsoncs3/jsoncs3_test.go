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

package jsoncs3_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	groupv1beta1 "github.com/cs3org/go-cs3apis/cs3/identity/group/v1beta1"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	collaboration "github.com/cs3org/go-cs3apis/cs3/sharing/collaboration/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	providerv1beta1 "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	ctxpkg "github.com/cs3org/reva/v2/pkg/ctx"
	"github.com/cs3org/reva/v2/pkg/share/manager/jsoncs3"
	"github.com/cs3org/reva/v2/pkg/share/manager/jsoncs3/sharecache"
	"github.com/cs3org/reva/v2/pkg/storage/utils/metadata"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Jsoncs3", func() {
	var (
		user1 = &userpb.User{
			Id: &userpb.UserId{
				Idp:      "https://localhost:9200",
				OpaqueId: "admin",
			},
		}
		user2 = &userpb.User{
			Id: &userpb.UserId{
				Idp:      "https://localhost:9200",
				OpaqueId: "einstein",
			},
		}

		sharedResource = &providerv1beta1.ResourceInfo{
			Id: &providerv1beta1.ResourceId{
				StorageId: "storageid",
				SpaceId:   "spaceid",
				OpaqueId:  "opaqueid",
			},
		}

		sharedResource2 = &providerv1beta1.ResourceInfo{
			Id: &providerv1beta1.ResourceId{
				StorageId: "storageid2",
				SpaceId:   "spaceid2",
				OpaqueId:  "opaqueid2",
			},
		}

		grantee = &userpb.User{
			Id:     user2.Id,
			Groups: []string{"users"},
		}
		readPermissions = &provider.ResourcePermissions{
			GetPath:              true,
			InitiateFileDownload: true,
			ListFileVersions:     true,
			ListContainer:        true,
			Stat:                 true,
		}
		writePermissions = &provider.ResourcePermissions{
			GetPath:              true,
			InitiateFileDownload: true,
			InitiateFileUpload:   true,
			ListFileVersions:     true,
			ListContainer:        true,
			Stat:                 true,
		}
		grant = &collaboration.ShareGrant{
			Grantee: &provider.Grantee{
				Type: provider.GranteeType_GRANTEE_TYPE_USER,
				Id:   &provider.Grantee_UserId{UserId: grantee.GetId()},
			},
			Permissions: &collaboration.SharePermissions{
				Permissions: readPermissions,
			},
		}

		groupGrant = &collaboration.ShareGrant{
			Grantee: &provider.Grantee{
				Type: provider.GranteeType_GRANTEE_TYPE_GROUP,
				Id: &provider.Grantee_GroupId{GroupId: &groupv1beta1.GroupId{
					OpaqueId: "group1",
				}},
			},
			Permissions: &collaboration.SharePermissions{
				Permissions: readPermissions,
			},
		}
		storage metadata.Storage
		tmpdir  string
		m       *jsoncs3.Manager

		ctx        = ctxpkg.ContextSetUser(context.Background(), user1)
		granteeCtx = ctxpkg.ContextSetUser(context.Background(), user2)
		otherCtx   = ctxpkg.ContextSetUser(context.Background(), &userpb.User{Id: &userpb.UserId{OpaqueId: "otheruser"}})

		// helper functions
		shareBykey = func(key *collaboration.ShareKey) *collaboration.Share {
			s, err := m.GetShare(ctx, &collaboration.ShareReference{
				Spec: &collaboration.ShareReference_Key{
					Key: key,
				},
			})
			ExpectWithOffset(1, err).ToNot(HaveOccurred())
			ExpectWithOffset(1, s).ToNot(BeNil())
			return s
		}
	)

	BeforeEach(func() {
		var err error
		tmpdir, err = ioutil.TempDir("", "jsoncs3-test")
		Expect(err).ToNot(HaveOccurred())

		err = os.MkdirAll(tmpdir, 0755)
		Expect(err).ToNot(HaveOccurred())

		storage, err = metadata.NewDiskStorage(tmpdir)
		Expect(err).ToNot(HaveOccurred())

		m, err = jsoncs3.New(storage)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		if tmpdir != "" {
			os.RemoveAll(tmpdir)
		}
	})

	Describe("Share", func() {
		It("fails if the share already exists", func() {
			_, err := m.Share(ctx, sharedResource, grant)
			Expect(err).ToNot(HaveOccurred())
			_, err = m.Share(ctx, sharedResource, grant)
			Expect(err).To(HaveOccurred())
		})

		It("creates a user share", func() {
			share, err := m.Share(ctx, sharedResource, grant)
			Expect(err).ToNot(HaveOccurred())

			Expect(share).ToNot(BeNil())
			Expect(share.ResourceId).To(Equal(sharedResource.Id))
		})

		It("creates a group share", func() {
			share, err := m.Share(ctx, sharedResource, groupGrant)
			Expect(err).ToNot(HaveOccurred())

			Expect(share).ToNot(BeNil())
			Expect(share.ResourceId).To(Equal(sharedResource.Id))
		})

		It("persists the share", func() {
			_, err := m.Share(ctx, sharedResource, grant)
			Expect(err).ToNot(HaveOccurred())

			s := shareBykey(&collaboration.ShareKey{
				ResourceId: sharedResource.Id,
				Grantee:    grant.Grantee,
			})
			Expect(s).ToNot(BeNil())

			m, err = jsoncs3.New(storage) // Reset in-memory cache
			Expect(err).ToNot(HaveOccurred())

			s = shareBykey(&collaboration.ShareKey{
				ResourceId: sharedResource.Id,
				Grantee:    grant.Grantee,
			})
			Expect(s).ToNot(BeNil())
		})
	})

	Context("with an existing share", func() {
		var (
			share    *collaboration.Share
			shareRef *collaboration.ShareReference
		)

		BeforeEach(func() {
			var err error
			share, err = m.Share(ctx, sharedResource, grant)
			Expect(err).ToNot(HaveOccurred())

			shareRef = &collaboration.ShareReference{
				Spec: &collaboration.ShareReference_Id{
					Id: &collaboration.ShareId{
						OpaqueId: share.Id.OpaqueId,
					},
				},
			}
		})

		Describe("GetShare", func() {
			It("handles unknown ids", func() {
				s, err := m.GetShare(ctx, &collaboration.ShareReference{
					Spec: &collaboration.ShareReference_Id{
						Id: &collaboration.ShareId{
							OpaqueId: "unknown-id",
						},
					},
				})
				Expect(s).To(BeNil())
				Expect(err).To(HaveOccurred())
			})

			It("handles unknown keys", func() {
				s, err := m.GetShare(ctx, &collaboration.ShareReference{
					Spec: &collaboration.ShareReference_Key{
						Key: &collaboration.ShareKey{
							ResourceId: &providerv1beta1.ResourceId{
								OpaqueId: "unknown",
							},
							Grantee: grant.Grantee,
						},
					},
				})
				Expect(s).To(BeNil())
				Expect(err).To(HaveOccurred())
			})

			It("retrieves an existing share by id", func() {
				s, err := m.GetShare(ctx, shareRef)
				Expect(err).ToNot(HaveOccurred())
				Expect(s).ToNot(BeNil())
				Expect(share.ResourceId).To(Equal(sharedResource.Id))
			})

			It("retrieves an existing share by key", func() {
				s := shareBykey(&collaboration.ShareKey{
					ResourceId: sharedResource.Id,
					Grantee:    grant.Grantee,
				})
				Expect(s.ResourceId).To(Equal(sharedResource.Id))
				Expect(s.Id.OpaqueId).To(Equal(share.Id.OpaqueId))
			})

			It("reloads the provider cache when it is outdated", func() {
				s, err := m.GetShare(ctx, shareRef)
				Expect(err).ToNot(HaveOccurred())
				Expect(s).ToNot(BeNil())
				Expect(s.Permissions.Permissions.InitiateFileUpload).To(BeFalse())

				cache := m.Cache.Providers["storageid"].Spaces["spaceid"]
				cache.Shares[share.Id.OpaqueId].Permissions.Permissions.InitiateFileUpload = true
				bytes, err := json.Marshal(cache)
				Expect(err).ToNot(HaveOccurred())
				storage.SimpleUpload(context.Background(), "storages/storageid/spaceid.json", bytes)
				Expect(err).ToNot(HaveOccurred())

				m.CreatedCache.UserShares["admin"].Mtime = time.Time{} // trigger reload
				s, err = m.GetShare(ctx, shareRef)
				Expect(err).ToNot(HaveOccurred())
				Expect(s).ToNot(BeNil())
				Expect(s.Permissions.Permissions.InitiateFileUpload).To(BeTrue())
			})

			It("loads the cache when it doesn't have an entry", func() {
				err := m.Cache.Persist(context.Background(), "storageid", "spaceid")
				Expect(err).ToNot(HaveOccurred())

				m, err = jsoncs3.New(storage) // Reset in-memory cache
				Expect(err).ToNot(HaveOccurred())

				s, err := m.GetShare(ctx, shareRef)
				Expect(err).ToNot(HaveOccurred())
				Expect(s).ToNot(BeNil())
			})

			It("does not return other users' shares", func() {
				s, err := m.GetShare(otherCtx, shareRef)
				Expect(err).To(HaveOccurred())
				Expect(s).To(BeNil())
			})
		})

		Describe("UnShare", func() {
			It("does not remove shares of other users", func() {
				err := m.Unshare(otherCtx, &collaboration.ShareReference{
					Spec: &collaboration.ShareReference_Id{
						Id: &collaboration.ShareId{
							OpaqueId: share.Id.OpaqueId,
						},
					},
				})

				Expect(err).To(HaveOccurred())
			})

			It("removes an existing share", func() {
				err := m.Unshare(ctx, &collaboration.ShareReference{
					Spec: &collaboration.ShareReference_Id{
						Id: &collaboration.ShareId{
							OpaqueId: share.Id.OpaqueId,
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				s, err := m.GetShare(ctx, &collaboration.ShareReference{
					Spec: &collaboration.ShareReference_Key{
						Key: &collaboration.ShareKey{
							ResourceId: sharedResource.Id,
							Grantee:    grant.Grantee,
						},
					},
				})
				Expect(err).To(HaveOccurred())
				Expect(s).To(BeNil())
			})

			It("removes an existing share from the storage", func() {
				err := m.Unshare(ctx, &collaboration.ShareReference{
					Spec: &collaboration.ShareReference_Id{
						Id: &collaboration.ShareId{
							OpaqueId: share.Id.OpaqueId,
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				m, err = jsoncs3.New(storage) // Reset in-memory cache
				Expect(err).ToNot(HaveOccurred())

				s, err := m.GetShare(ctx, &collaboration.ShareReference{
					Spec: &collaboration.ShareReference_Key{
						Key: &collaboration.ShareKey{
							ResourceId: sharedResource.Id,
							Grantee:    grant.Grantee,
						},
					},
				})
				Expect(err).To(HaveOccurred())
				Expect(s).To(BeNil())
			})
		})

		Describe("UpdateShare", func() {
			It("does not update shares of other users", func() {
				_, err := m.UpdateShare(otherCtx, &collaboration.ShareReference{
					Spec: &collaboration.ShareReference_Id{
						Id: &collaboration.ShareId{
							OpaqueId: share.Id.OpaqueId,
						},
					},
				}, &collaboration.SharePermissions{
					Permissions: writePermissions,
				})
				Expect(err).To(HaveOccurred())
			})

			It("updates an existing share", func() {
				s := shareBykey(&collaboration.ShareKey{
					ResourceId: sharedResource.Id,
					Grantee:    grant.Grantee,
				})
				Expect(s.GetPermissions().GetPermissions()).To(Equal(readPermissions))

				// enhance privileges
				us, err := m.UpdateShare(ctx, &collaboration.ShareReference{
					Spec: &collaboration.ShareReference_Id{
						Id: &collaboration.ShareId{
							OpaqueId: share.Id.OpaqueId,
						},
					},
				}, &collaboration.SharePermissions{
					Permissions: writePermissions,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(us).ToNot(BeNil())
				Expect(us.GetPermissions().GetPermissions()).To(Equal(writePermissions))

				s = shareBykey(&collaboration.ShareKey{
					ResourceId: sharedResource.Id,
					Grantee:    grant.Grantee,
				})
				Expect(s.GetPermissions().GetPermissions()).To(Equal(writePermissions))

				// reduce privileges
				us, err = m.UpdateShare(ctx, &collaboration.ShareReference{
					Spec: &collaboration.ShareReference_Id{
						Id: &collaboration.ShareId{
							OpaqueId: share.Id.OpaqueId,
						},
					},
				}, &collaboration.SharePermissions{
					Permissions: readPermissions,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(us).ToNot(BeNil())
				Expect(us.GetPermissions().GetPermissions()).To(Equal(readPermissions))

				s = shareBykey(&collaboration.ShareKey{
					ResourceId: sharedResource.Id,
					Grantee:    grant.Grantee,
				})
				Expect(s.GetPermissions().GetPermissions()).To(Equal(readPermissions))
			})
		})
		Describe("ListShares", func() {
			It("lists an existing share", func() {
				shares, err := m.ListShares(ctx, nil)
				Expect(err).ToNot(HaveOccurred())
				Expect(shares).To(HaveLen(1))

				Expect(shares[0].Id).To(Equal(share.Id))
			})

			It("uses the data from the storage after reload", func() {
				shares, err := m.ListShares(ctx, nil)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(shares)).To(Equal(1))

				// Add a second cache to the provider cache so it can be referenced
				m.Cache.Add(ctx, "storageid", "spaceid", "storageid$spaceid!secondshare", &collaboration.Share{
					Creator: user1.Id,
				})

				cache := sharecache.UserShareCache{
					Mtime: time.Now(),
					UserShares: map[string]*sharecache.SpaceShareIDs{
						"storageid$spaceid": {
							Mtime: time.Now(),
							IDs: map[string]struct{}{
								shares[0].Id.OpaqueId:           {},
								"storageid$spaceid!secondshare": {},
							},
						},
					},
				}
				bytes, err := json.Marshal(cache)
				Expect(err).ToNot(HaveOccurred())
				err = os.WriteFile(filepath.Join(tmpdir, "users/admin/created.json"), bytes, 0x755)
				Expect(err).ToNot(HaveOccurred())

				m.CreatedCache.UserShares["admin"].Mtime = time.Time{} // trigger reload
				shares, err = m.ListShares(ctx, nil)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(shares)).To(Equal(2))
			})
		})

		Describe("ListReceivedShares", func() {
			PIt("filters by owner")
			PIt("filters by creator")
			PIt("filters by grantee type")

			It("filters by resource id", func() {
				share2, err := m.Share(ctx, sharedResource2, grant)
				Expect(err).ToNot(HaveOccurred())

				received, err := m.ListReceivedShares(granteeCtx, []*collaboration.Filter{})
				Expect(err).ToNot(HaveOccurred())
				Expect(len(received)).To(Equal(2))

				received, err = m.ListReceivedShares(granteeCtx, []*collaboration.Filter{
					{
						Type: collaboration.Filter_TYPE_RESOURCE_ID,
						Term: &collaboration.Filter_ResourceId{
							ResourceId: sharedResource.Id,
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(len(received)).To(Equal(1))
				Expect(received[0].Share.ResourceId).To(Equal(sharedResource.Id))
				Expect(received[0].State).To(Equal(collaboration.ShareState_SHARE_STATE_PENDING))
				Expect(received[0].Share.Id).To(Equal(share.Id))

				received, err = m.ListReceivedShares(granteeCtx, []*collaboration.Filter{
					{
						Type: collaboration.Filter_TYPE_RESOURCE_ID,
						Term: &collaboration.Filter_ResourceId{
							ResourceId: sharedResource2.Id,
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(len(received)).To(Equal(1))
				Expect(received[0].Share.ResourceId).To(Equal(sharedResource2.Id))
				Expect(received[0].State).To(Equal(collaboration.ShareState_SHARE_STATE_PENDING))
				Expect(received[0].Share.Id).To(Equal(share2.Id))

			})

			It("lists the received shares", func() {
				received, err := m.ListReceivedShares(granteeCtx, []*collaboration.Filter{})
				Expect(err).ToNot(HaveOccurred())
				Expect(len(received)).To(Equal(1))
				Expect(received[0].Share.ResourceId).To(Equal(sharedResource.Id))
				Expect(received[0].State).To(Equal(collaboration.ShareState_SHARE_STATE_PENDING))
			})
		})

		Describe("GetReceivedShare", func() {
			It("gets the state", func() {
				rs, err := m.GetReceivedShare(granteeCtx, &collaboration.ShareReference{
					Spec: &collaboration.ShareReference_Id{
						Id: share.Id,
					},
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(rs.State).To(Equal(collaboration.ShareState_SHARE_STATE_PENDING))
			})
		})

		Describe("UpdateReceivedShare", func() {
			It("updates the state", func() {
				rs, err := m.GetReceivedShare(granteeCtx, &collaboration.ShareReference{
					Spec: &collaboration.ShareReference_Id{
						Id: share.Id,
					},
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(rs.State).To(Equal(collaboration.ShareState_SHARE_STATE_PENDING))

				rs.State = collaboration.ShareState_SHARE_STATE_ACCEPTED
				rs, err = m.UpdateReceivedShare(granteeCtx, rs, &fieldmaskpb.FieldMask{Paths: []string{"state"}})
				Expect(err).ToNot(HaveOccurred())
				Expect(rs.State).To(Equal(collaboration.ShareState_SHARE_STATE_ACCEPTED))

				rs, err = m.GetReceivedShare(granteeCtx, &collaboration.ShareReference{
					Spec: &collaboration.ShareReference_Id{
						Id: share.Id,
					},
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(rs.State).To(Equal(collaboration.ShareState_SHARE_STATE_ACCEPTED))
			})
		})
	})
})
