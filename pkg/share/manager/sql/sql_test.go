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

package sql_test

import (
	"context"
	"database/sql"
	"io/ioutil"
	"os"

	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	collaboration "github.com/cs3org/go-cs3apis/cs3/sharing/collaboration/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	ruser "github.com/cs3org/reva/pkg/ctx"
	"github.com/cs3org/reva/pkg/share"
	sqlmanager "github.com/cs3org/reva/pkg/share/manager/sql"
	mocks "github.com/cs3org/reva/pkg/share/manager/sql/mocks"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SQL manager", func() {
	var (
		mgr        share.Manager
		ctx        context.Context
		testDbFile *os.File
		sqldb      *sql.DB

		loginAs = func(user *userpb.User) {
			ctx = ruser.ContextSetUser(context.Background(), user)
		}
		admin = &userpb.User{
			Id: &userpb.UserId{
				Idp:      "idp",
				OpaqueId: "userid",
				Type:     userpb.UserType_USER_TYPE_PRIMARY,
			},
			Username: "admin",
		}
		otherUser = &userpb.User{
			Id: &userpb.UserId{
				Idp:      "idp",
				OpaqueId: "userid",
				Type:     userpb.UserType_USER_TYPE_PRIMARY,
			},
			Username: "einstein",
			Groups:   []string{"users"},
		}

		shareRef = &collaboration.ShareReference{Spec: &collaboration.ShareReference_Id{
			Id: &collaboration.ShareId{
				OpaqueId: "1",
			},
		}}

		insertShare = func(shareType int, owner string, grantee string, parent int, source int, fileTarget string, permissions int, accepted int) error {
			var parentVal interface{}
			if parent >= 0 {
				parentVal = parent
			}
			stmtString := "INSERT INTO oc_share (share_type,uid_owner,uid_initiator,item_type,item_source,file_source,parent,permissions,stime,share_with,file_target) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
			stmtValues := []interface{}{shareType, owner, owner, "folder", source, source, parentVal, permissions, 1631779730, grantee, fileTarget}

			stmt, err := sqldb.Prepare(stmtString)
			if err != nil {
				return err
			}
			result, err := stmt.Exec(stmtValues...)
			if err != nil {
				return err
			}
			_, err = result.LastInsertId()
			return err
		}
	)

	AfterEach(func() {
		os.Remove(testDbFile.Name())
	})

	BeforeEach(func() {
		var err error
		testDbFile, err = ioutil.TempFile("", "example")
		Expect(err).ToNot(HaveOccurred())

		dbData, err := ioutil.ReadFile("test.db")
		Expect(err).ToNot(HaveOccurred())

		_, err = testDbFile.Write(dbData)
		Expect(err).ToNot(HaveOccurred())
		err = testDbFile.Close()
		Expect(err).ToNot(HaveOccurred())

		sqldb, err = sql.Open("sqlite3", testDbFile.Name())
		Expect(err).ToNot(HaveOccurred())

		userConverter := &mocks.UserConverter{}
		userConverter.On("UserIDToUserName", mock.Anything, mock.Anything).Return("username", nil)
		userConverter.On("UserNameToUserID", mock.Anything, mock.Anything).Return(
			func(_ context.Context, username string) *userpb.UserId {
				return &userpb.UserId{
					OpaqueId: username,
				}
			},
			func(_ context.Context, username string) error { return nil })
		mgr, err = sqlmanager.New("sqlite3", sqldb, "abcde", userConverter)
		Expect(err).ToNot(HaveOccurred())

		loginAs(admin)
	})

	It("creates manager instances", func() {
		Expect(mgr).ToNot(BeNil())
	})

	Describe("GetShare", func() {
		It("returns the share", func() {
			share, err := mgr.GetShare(ctx, shareRef)
			Expect(err).ToNot(HaveOccurred())
			Expect(share).ToNot(BeNil())
		})

		It("returns an error if the share does not exis", func() {
			share, err := mgr.GetShare(ctx, &collaboration.ShareReference{Spec: &collaboration.ShareReference_Id{
				Id: &collaboration.ShareId{
					OpaqueId: "2",
				},
			}})
			Expect(err).To(HaveOccurred())
			Expect(share).To(BeNil())
		})
	})

	Describe("Share", func() {
		It("creates a share", func() {
			grant := &collaboration.ShareGrant{
				Grantee: &provider.Grantee{
					Type: provider.GranteeType_GRANTEE_TYPE_USER,
					Id: &provider.Grantee_UserId{UserId: &user.UserId{
						OpaqueId: "someone",
					}},
				},
				Permissions: &collaboration.SharePermissions{
					Permissions: &provider.ResourcePermissions{
						GetPath:              true,
						InitiateFileDownload: true,
						ListFileVersions:     true,
						ListContainer:        true,
						Stat:                 true,
					},
				},
			}
			info := &provider.ResourceInfo{
				Id: &provider.ResourceId{
					StorageId: "/",
					OpaqueId:  "something",
				},
			}
			share, err := mgr.Share(ctx, info, grant)

			Expect(err).ToNot(HaveOccurred())
			Expect(share).ToNot(BeNil())
			Expect(share.Id.OpaqueId).To(Equal("2"))
		})
	})

	Describe("ListShares", func() {
		It("lists shares", func() {
			shares, err := mgr.ListShares(ctx, []*collaboration.Filter{})
			Expect(err).ToNot(HaveOccurred())
			Expect(len(shares)).To(Equal(1))

			shares, err = mgr.ListShares(ctx, []*collaboration.Filter{
				share.ResourceIDFilter(&provider.ResourceId{
					StorageId: "/",
					OpaqueId:  "somethingElse",
				}),
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(len(shares)).To(Equal(0))
		})
	})

	Describe("ListReceivedShares", func() {
		Context("with an accepted group share", func() {
			It("lists the group share too", func() {
				loginAs(otherUser)
				err := insertShare(
					1,         // group share
					"admin",   // owner/initiator
					"users",   // grantee
					-1,        // parent
					20,        // source
					"/shared", // file_target
					31,        // permissions,
					0,         // accepted
				)
				Expect(err).ToNot(HaveOccurred())

				shares, err := mgr.ListReceivedShares(ctx, []*collaboration.Filter{})
				Expect(err).ToNot(HaveOccurred())
				Expect(len(shares)).To(Equal(2))
			})

			It("does not lists group shares named like the user", func() {
				loginAs(otherUser)
				err := insertShare(
					1,          // group share
					"admin",    // owner/initiator
					"einstein", // grantee
					-1,         // parent
					20,         // source
					"/shared",  // file_target
					31,         // permissions,
					0,          // accepted
				)
				Expect(err).ToNot(HaveOccurred())

				shares, err := mgr.ListReceivedShares(ctx, []*collaboration.Filter{})
				Expect(err).ToNot(HaveOccurred())
				Expect(len(shares)).To(Equal(1))
			})
		})

		It("lists received shares", func() {
			loginAs(otherUser)
			shares, err := mgr.ListReceivedShares(ctx, []*collaboration.Filter{})
			Expect(err).ToNot(HaveOccurred())
			Expect(len(shares)).To(Equal(1))
		})
	})

	Describe("GetReceivedShare", func() {
		It("returns the received share", func() {
			loginAs(otherUser)
			share, err := mgr.GetReceivedShare(ctx, shareRef)
			Expect(err).ToNot(HaveOccurred())
			Expect(share).ToNot(BeNil())
		})
	})

	Describe("UpdateReceivedShare", func() {
		It("returns an error when no valid field is set in the mask", func() {
			loginAs(otherUser)

			share, err := mgr.GetReceivedShare(ctx, shareRef)
			Expect(err).ToNot(HaveOccurred())
			Expect(share).ToNot(BeNil())
			Expect(share.State).To(Equal(collaboration.ShareState_SHARE_STATE_ACCEPTED))

			share.State = collaboration.ShareState_SHARE_STATE_REJECTED

			_, err = mgr.UpdateReceivedShare(ctx, share, &fieldmaskpb.FieldMask{Paths: []string{"foo"}})
			Expect(err).To(HaveOccurred())
		})

		It("updates the state when the state is set in the mask", func() {
			loginAs(otherUser)

			share, err := mgr.GetReceivedShare(ctx, shareRef)
			Expect(err).ToNot(HaveOccurred())
			Expect(share).ToNot(BeNil())
			Expect(share.State).To(Equal(collaboration.ShareState_SHARE_STATE_ACCEPTED))

			share.State = collaboration.ShareState_SHARE_STATE_REJECTED

			share, err = mgr.UpdateReceivedShare(ctx, share, &fieldmaskpb.FieldMask{Paths: []string{"mount_point"}})
			Expect(err).ToNot(HaveOccurred())
			Expect(share.State).To(Equal(collaboration.ShareState_SHARE_STATE_ACCEPTED))

			share.State = collaboration.ShareState_SHARE_STATE_REJECTED
			share, err = mgr.UpdateReceivedShare(ctx, share, &fieldmaskpb.FieldMask{Paths: []string{"state"}})
			Expect(err).ToNot(HaveOccurred())
			Expect(share.State).To(Equal(collaboration.ShareState_SHARE_STATE_REJECTED))

			share, err = mgr.GetReceivedShare(ctx, shareRef)
			Expect(err).ToNot(HaveOccurred())
			Expect(share).ToNot(BeNil())
			Expect(share.State).To(Equal(collaboration.ShareState_SHARE_STATE_REJECTED))
		})

		It("updates the mount_point when the mount_point is set in the mask", func() {
			loginAs(otherUser)

			share, err := mgr.GetReceivedShare(ctx, shareRef)
			Expect(err).ToNot(HaveOccurred())
			Expect(share).ToNot(BeNil())
			Expect(share.State).To(Equal(collaboration.ShareState_SHARE_STATE_ACCEPTED))

			share.MountPoint = &provider.Reference{Path: "foo"}

			share, err = mgr.UpdateReceivedShare(ctx, share, &fieldmaskpb.FieldMask{Paths: []string{"state"}})
			Expect(err).ToNot(HaveOccurred())
			Expect(share.MountPoint.Path).To(Equal("shared"))

			share.MountPoint = &provider.Reference{Path: "foo"}
			share, err = mgr.UpdateReceivedShare(ctx, share, &fieldmaskpb.FieldMask{Paths: []string{"mount_point"}})
			Expect(err).ToNot(HaveOccurred())
			Expect(share.MountPoint.Path).To(Equal("foo"))

			share, err = mgr.GetReceivedShare(ctx, shareRef)
			Expect(err).ToNot(HaveOccurred())
			Expect(share).ToNot(BeNil())
			Expect(share.MountPoint.Path).To(Equal("foo"))
		})
	})

	Describe("Unshare", func() {
		It("deletes shares", func() {
			loginAs(otherUser)
			shares, err := mgr.ListReceivedShares(ctx, []*collaboration.Filter{})
			Expect(err).ToNot(HaveOccurred())
			Expect(len(shares)).To(Equal(1))

			loginAs(admin)
			err = mgr.Unshare(ctx, &collaboration.ShareReference{Spec: &collaboration.ShareReference_Id{
				Id: &collaboration.ShareId{
					OpaqueId: shares[0].Share.Id.OpaqueId,
				},
			}})
			Expect(err).ToNot(HaveOccurred())

			loginAs(otherUser)
			shares, err = mgr.ListReceivedShares(ctx, []*collaboration.Filter{})
			Expect(err).ToNot(HaveOccurred())
			Expect(len(shares)).To(Equal(0))
		})
	})

	Describe("UpdateShare", func() {
		It("updates permissions", func() {
			share, err := mgr.GetShare(ctx, shareRef)
			Expect(err).ToNot(HaveOccurred())
			Expect(share.Permissions.Permissions.Delete).To(BeTrue())

			share, err = mgr.UpdateShare(ctx, shareRef, &collaboration.SharePermissions{
				Permissions: &provider.ResourcePermissions{
					InitiateFileUpload: true,
					RestoreFileVersion: true,
					RestoreRecycleItem: true,
				}})
			Expect(err).ToNot(HaveOccurred())
			Expect(share.Permissions.Permissions.Delete).To(BeFalse())

			share, err = mgr.GetShare(ctx, shareRef)
			Expect(err).ToNot(HaveOccurred())
			Expect(share.Permissions.Permissions.Delete).To(BeFalse())
		})
	})
})
