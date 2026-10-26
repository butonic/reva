// Copyright 2026 OpenCloud GmbH <mail@opencloud.eu>
// SPDX-License-Identifier: Apache-2.0

package blobstore_test

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/xattr"

	posixblobstore "github.com/opencloud-eu/reva/v2/pkg/storage/fs/posix/blobstore"
	posixhelpers "github.com/opencloud-eu/reva/v2/pkg/storage/fs/posix/testhelpers"
	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/metadata/prefixes"
	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/node"
)

var _ = Describe("Blobstore with a source on the node's filesystem", func() {
	var (
		env    *posixhelpers.TestEnv
		bs     *posixblobstore.Blobstore
		n      *node.Node
		data   []byte
		source string
	)

	BeforeEach(func() {
		var err error
		env, err = posixhelpers.NewTestEnv(map[string]interface{}{})
		Expect(err).ToNot(HaveOccurred())

		bs, err = posixblobstore.New(env.Root)
		Expect(err).ToNot(HaveOccurred())

		n, err = env.CreateTestFile("blob.bin", "", env.SpaceRootRes.OpaqueId, env.SpaceRootRes.SpaceId, 1024)
		Expect(err).ToNot(HaveOccurred())

		data = make([]byte, 1024)
		_, err = rand.Read(data)
		Expect(err).ToNot(HaveOccurred())

		// outside the space, like the uploads dir, but on the same filesystem
		source = filepath.Join(env.Root, "upload-source")
		Expect(os.WriteFile(source, data, 0600)).To(Succeed())
	})

	AfterEach(func() {
		env.Cleanup()
	})

	It("moves the source onto the node with a single rename", func() {
		mtime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		Expect(n.SetXattrString(env.Ctx, prefixes.MTimeAttr, mtime.Format(time.RFC3339Nano))).To(Succeed())

		Expect(bs.Upload(n, source, "")).To(Succeed())

		Expect(os.ReadFile(n.InternalPath())).To(Equal(data))
		_, err := os.Stat(source)
		Expect(os.IsNotExist(err)).To(BeTrue(), "the source should have been moved")

		// nothing went through the space's tmp dir
		Expect(filepath.Glob(filepath.Join(n.SpaceRoot.InternalPath(), posixblobstore.TMPDir, "*"))).To(BeEmpty())

		// the blob carries the node's metadata and mtime
		id, err := xattr.Get(n.InternalPath(), prefixes.IDAttr)
		Expect(err).ToNot(HaveOccurred())
		Expect(string(id)).To(Equal(n.ID))
		fi, err := os.Stat(n.InternalPath())
		Expect(err).ToNot(HaveOccurred())
		Expect(fi.ModTime().UTC()).To(Equal(mtime))
	})

	It("does not go through the space's tmp dir", func() {
		// a file in the way: the tmp dir can't be created
		Expect(os.WriteFile(filepath.Join(n.SpaceRoot.InternalPath(), posixblobstore.TMPDir), nil, 0600)).To(Succeed())

		Expect(bs.Upload(n, source, "")).To(Succeed())

		Expect(os.ReadFile(n.InternalPath())).To(Equal(data))
	})

	It("leaves the source alone when the node can't be read", func() {
		Expect(os.Remove(n.InternalPath())).To(Succeed())

		Expect(bs.Upload(n, source, "")).ToNot(Succeed())

		Expect(os.ReadFile(source)).To(Equal(data))
	})
})
