package iavl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	dbm "github.com/cosmos/iavl/db"
	"github.com/cosmos/iavl/fastnode"
)

type appendReadDB struct {
	dbm.DB
	appendCalls int
}

func (d *appendReadDB) GetAppend(key, dst []byte) ([]byte, error) {
	d.appendCalls++
	val, err := d.Get(key)
	if err != nil || val == nil {
		return nil, err
	}
	dst = append(dst[:0], val...)
	return dst, nil
}

func TestEffectiveNodeCacheSize_CapsAppendReadDB(t *testing.T) {
	base := dbm.NewMemDB()
	wrapped := &appendReadDB{DB: base}

	require.Equal(t, appendReadNodeCacheSizeCap, effectiveNodeCacheSize(wrapped, appendReadNodeCacheSizeCap+1))
	require.Equal(t, appendReadNodeCacheSizeCap, effectiveNodeCacheSize(wrapped, 781250))
	require.Equal(t, appendReadNodeCacheSizeCap-1, effectiveNodeCacheSize(wrapped, appendReadNodeCacheSizeCap-1))
	require.Equal(t, 0, effectiveNodeCacheSize(wrapped, 0))
	require.Equal(t, -1, effectiveNodeCacheSize(wrapped, -1))
}

func TestEffectiveNodeCacheSize_PreservesRegularDB(t *testing.T) {
	base := dbm.NewMemDB()

	require.Equal(t, 781250, effectiveNodeCacheSize(base, 781250))
	require.Equal(t, appendReadNodeCacheSizeCap+1, effectiveNodeCacheSize(base, appendReadNodeCacheSizeCap+1))
}

func descendToLeaf(t *testing.T, ndb *nodeDB, node *Node) *Node {
	t.Helper()
	cur := node
	for !cur.isLeaf() {
		require.NotNil(t, cur.leftNodeKey)
		next, err := ndb.GetNode(cur.leftNodeKey)
		require.NoError(t, err)
		cur = next
	}
	return cur
}

func TestNodeDBGetNode_GetAppendMaterializesOwnedBytes(t *testing.T) {
	base := dbm.NewMemDB()
	tree := NewMutableTree(base, 0, false, NewNopLogger())
	for i := 0; i < 32; i++ {
		updated, err := tree.Set([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%03d", i)))
		require.NoError(t, err)
		require.False(t, updated)
	}
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	wrapped := &appendReadDB{DB: base}
	reopened := NewMutableTree(wrapped, 0, false, NewNopLogger())
	_, err = reopened.LoadVersion(version)
	require.NoError(t, err)

	rootKey, err := reopened.ndb.GetRoot(version)
	require.NoError(t, err)
	root, err := reopened.ndb.GetNode(rootKey)
	require.NoError(t, err)
	require.False(t, root.isLeaf())

	rootKeyBefore := append([]byte(nil), root.key...)
	rootHashBefore := append([]byte(nil), root.hash...)
	leftKeyBefore := append([]byte(nil), root.leftNodeKey...)
	rightKeyBefore := append([]byte(nil), root.rightNodeKey...)

	leaf := descendToLeaf(t, reopened.ndb, root)
	leafKeyBefore := append([]byte(nil), leaf.key...)
	leafValBefore := append([]byte(nil), leaf.value...)

	_, err = reopened.ndb.GetNode(leftKeyBefore)
	require.NoError(t, err)
	_, err = reopened.ndb.GetNode(rightKeyBefore)
	require.NoError(t, err)

	require.Greater(t, wrapped.appendCalls, 0)
	require.Equal(t, rootKeyBefore, root.key)
	require.Equal(t, rootHashBefore, root.hash)
	require.Equal(t, leftKeyBefore, root.leftNodeKey)
	require.Equal(t, rightKeyBefore, root.rightNodeKey)
	require.Equal(t, leafKeyBefore, leaf.key)
	require.Equal(t, leafValBefore, leaf.value)
}

func TestNodeDBGetNode_GetAppendSkipsLeafCacheAdmission(t *testing.T) {
	base := dbm.NewMemDB()
	tree := NewMutableTree(base, 0, false, NewNopLogger())
	for i := 0; i < 32; i++ {
		updated, err := tree.Set([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("value-%03d-with-payload", i)))
		require.NoError(t, err)
		require.False(t, updated)
	}
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	wrapped := &appendReadDB{DB: base}
	stat := &Statistics{}
	reopened := NewMutableTree(wrapped, 128, false, NewNopLogger(), StatOption(stat))
	_, err = reopened.LoadVersion(version)
	require.NoError(t, err)

	rootKey, err := reopened.ndb.GetRoot(version)
	require.NoError(t, err)
	root, err := reopened.ndb.GetNode(rootKey)
	require.NoError(t, err)
	require.False(t, root.isLeaf())
	require.True(t, reopened.ndb.nodeCache.Has(root.GetKey()), "scratch-loaded internal nodes remain cache-admitted")
	appendCallsAfterRoot := wrapped.appendCalls
	_, err = reopened.ndb.GetNode(root.GetKey())
	require.NoError(t, err)
	require.Equal(t, appendCallsAfterRoot, wrapped.appendCalls, "cache-admitted internal node should not read again")

	leaf := descendToLeaf(t, reopened.ndb, root)
	leafKey := append([]byte(nil), leaf.GetKey()...)
	require.False(t, reopened.ndb.nodeCache.Has(leafKey), "scratch-loaded leaves should not be retained in node cache")

	missesBefore := stat.GetCacheMissCnt()
	appendCallsBeforeLeaf := wrapped.appendCalls
	secondLeaf, err := reopened.ndb.GetNode(leafKey)
	require.NoError(t, err)
	require.Equal(t, leaf.value, secondLeaf.value)
	require.Greater(t, wrapped.appendCalls, appendCallsBeforeLeaf, "uncached leaf read should hit append path again")
	require.Greater(t, stat.GetCacheMissCnt(), missesBefore, "uncached leaf read should miss again")
	require.False(t, reopened.ndb.nodeCache.Has(leafKey), "repeated scratch-loaded leaf reads remain uncached")
}

func TestNodeDBGetFastNode_GetAppendMaterializesOwnedBytes(t *testing.T) {
	base := dbm.NewMemDB()
	ndb := newNodeDB(base, 0, DefaultOptions(), NewNopLogger())
	require.NoError(t, ndb.SaveFastNodeNoCache(fastnode.NewNode([]byte("key-a"), []byte("value-a"), 1)))
	require.NoError(t, ndb.SaveFastNodeNoCache(fastnode.NewNode([]byte("key-b"), []byte("value-b-with-longer-payload"), 1)))
	require.NoError(t, ndb.SetFastStorageVersionToBatch(1))
	require.NoError(t, ndb.Commit())

	wrapped := &appendReadDB{DB: base}
	reopened := newNodeDB(wrapped, 0, DefaultOptions(), NewNopLogger())
	fastNode, err := reopened.GetFastNode([]byte("key-a"))
	require.NoError(t, err)
	require.NotNil(t, fastNode)

	keyBefore := append([]byte(nil), fastNode.GetKey()...)
	valueBefore := append([]byte(nil), fastNode.GetValue()...)

	otherFastNode, err := reopened.GetFastNode([]byte("key-b"))
	require.NoError(t, err)
	require.NotNil(t, otherFastNode)

	require.Equal(t, 2, wrapped.appendCalls)
	require.Equal(t, keyBefore, fastNode.GetKey())
	require.Equal(t, valueBefore, fastNode.GetValue())
}

func TestNodeDBGetAppendReadScratchRetentionCap(t *testing.T) {
	ndb := &nodeDB{}

	small := make([]byte, 1, maxReadScratchCap)
	ndb.retainReadScratch(small)
	require.NotNil(t, ndb.readScratch)
	require.Len(t, ndb.readScratch, 0)
	require.Equal(t, maxReadScratchCap, cap(ndb.readScratch))

	oversize := make([]byte, 1, maxReadScratchCap+1)
	ndb.retainReadScratch(oversize)
	require.Nil(t, ndb.readScratch)
}

func TestMutableTreeLoadVersionWithGetAppendDB_CanSaveNextVersion(t *testing.T) {
	base := dbm.NewMemDB()
	tree := NewMutableTree(base, 0, false, NewNopLogger())
	for i := 0; i < 64; i++ {
		updated, err := tree.Set([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%03d", i)))
		require.NoError(t, err)
		require.False(t, updated)
	}
	_, version1, err := tree.SaveVersion()
	require.NoError(t, err)

	wrapped := &appendReadDB{DB: base}
	reopened := NewMutableTree(wrapped, 0, false, NewNopLogger())
	_, err = reopened.LoadVersion(version1)
	require.NoError(t, err)

	updated, err := reopened.Set([]byte("k999"), []byte("v999"))
	require.NoError(t, err)
	require.False(t, updated)
	_, version2, err := reopened.SaveVersion()
	require.NoError(t, err)

	require.Greater(t, wrapped.appendCalls, 0)

	verify := NewMutableTree(base, 0, false, NewNopLogger())
	_, err = verify.LoadVersion(version2)
	require.NoError(t, err)
	got, err := verify.Get([]byte("k999"))
	require.NoError(t, err)
	require.Equal(t, []byte("v999"), got)
	got, err = verify.Get([]byte("k012"))
	require.NoError(t, err)
	require.Equal(t, []byte("v012"), got)
}
