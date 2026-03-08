package iavl

import (
	"bytes"
	"testing"
	"time"

	corestore "cosmossdk.io/core/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbm "github.com/cosmos/iavl/db"
)

type delayedWriteDB struct {
	inner      dbm.DB
	writeDelay time.Duration
}

func (d *delayedWriteDB) Get(key []byte) ([]byte, error) { return d.inner.Get(key) }
func (d *delayedWriteDB) Has(key []byte) (bool, error)   { return d.inner.Has(key) }
func (d *delayedWriteDB) Iterator(start, end []byte) (corestore.Iterator, error) {
	return d.inner.Iterator(start, end)
}
func (d *delayedWriteDB) ReverseIterator(start, end []byte) (corestore.Iterator, error) {
	return d.inner.ReverseIterator(start, end)
}
func (d *delayedWriteDB) Close() error { return d.inner.Close() }
func (d *delayedWriteDB) NewBatch() corestore.Batch {
	return &delayedWriteBatch{inner: d.inner.NewBatch(), writeDelay: d.writeDelay}
}
func (d *delayedWriteDB) NewBatchWithSize(size int) corestore.Batch {
	return &delayedWriteBatch{inner: d.inner.NewBatchWithSize(size), writeDelay: d.writeDelay}
}

type delayedWriteBatch struct {
	inner      corestore.Batch
	writeDelay time.Duration
}

func (b *delayedWriteBatch) Set(key, value []byte) error { return b.inner.Set(key, value) }
func (b *delayedWriteBatch) Delete(key []byte) error     { return b.inner.Delete(key) }
func (b *delayedWriteBatch) Write() error {
	time.Sleep(b.writeDelay)
	return b.inner.Write()
}
func (b *delayedWriteBatch) WriteSync() error          { return b.inner.WriteSync() }
func (b *delayedWriteBatch) Close() error              { return b.inner.Close() }
func (b *delayedWriteBatch) GetByteSize() (int, error) { return b.inner.GetByteSize() }

type stagedVisibilityDB struct {
	visible         *dbm.MemDB
	staged          *dbm.MemDB
	checkpointCount int
}

func newStagedVisibilityDB() *stagedVisibilityDB {
	return &stagedVisibilityDB{
		visible: dbm.NewMemDB(),
		staged:  dbm.NewMemDB(),
	}
}

func (d *stagedVisibilityDB) Get(key []byte) ([]byte, error) { return d.visible.Get(key) }
func (d *stagedVisibilityDB) Has(key []byte) (bool, error)   { return d.visible.Has(key) }
func (d *stagedVisibilityDB) Iterator(start, end []byte) (corestore.Iterator, error) {
	return d.visible.Iterator(start, end)
}
func (d *stagedVisibilityDB) ReverseIterator(start, end []byte) (corestore.Iterator, error) {
	return d.visible.ReverseIterator(start, end)
}
func (d *stagedVisibilityDB) Close() error { return nil }
func (d *stagedVisibilityDB) NewBatch() corestore.Batch {
	return &stagedVisibilityBatch{db: d}
}
func (d *stagedVisibilityDB) NewBatchWithSize(size int) corestore.Batch {
	return &stagedVisibilityBatch{db: d}
}
func (d *stagedVisibilityDB) Checkpoint() error {
	iter, err := d.staged.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer iter.Close()
	nextVisible := dbm.NewMemDB()
	for ; iter.Valid(); iter.Next() {
		if err := nextVisible.Set(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	d.visible = nextVisible
	d.checkpointCount++
	return nil
}

type stagedVisibilityBatch struct {
	db     *stagedVisibilityDB
	ops    []stagedVisibilityOp
	closed bool
}

type stagedVisibilityOp struct {
	key   []byte
	value []byte
	del   bool
}

func (b *stagedVisibilityBatch) Set(key, value []byte) error {
	if b.closed {
		return ErrNoImport
	}
	b.ops = append(b.ops, stagedVisibilityOp{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	})
	return nil
}

func (b *stagedVisibilityBatch) Delete(key []byte) error {
	if b.closed {
		return ErrNoImport
	}
	b.ops = append(b.ops, stagedVisibilityOp{
		key: append([]byte(nil), key...),
		del: true,
	})
	return nil
}

func (b *stagedVisibilityBatch) Write() error     { return b.apply() }
func (b *stagedVisibilityBatch) WriteSync() error { return b.apply() }

func (b *stagedVisibilityBatch) apply() error {
	if b.closed {
		return ErrNoImport
	}
	for _, op := range b.ops {
		if op.del {
			if err := b.db.staged.Delete(op.key); err != nil {
				return err
			}
			continue
		}
		if err := b.db.staged.Set(op.key, op.value); err != nil {
			return err
		}
	}
	b.closed = true
	return nil
}

func (b *stagedVisibilityBatch) Close() error {
	b.closed = true
	b.ops = nil
	return nil
}

func (b *stagedVisibilityBatch) GetByteSize() (int, error) {
	size := 0
	for _, op := range b.ops {
		size += len(op.key) + len(op.value)
	}
	return size, nil
}

type syncVisibilityDB struct {
	visible *dbm.MemDB
	pending *dbm.MemDB
}

func newSyncVisibilityDB() *syncVisibilityDB {
	return &syncVisibilityDB{
		visible: dbm.NewMemDB(),
		pending: dbm.NewMemDB(),
	}
}

func (d *syncVisibilityDB) Get(key []byte) ([]byte, error) { return d.visible.Get(key) }
func (d *syncVisibilityDB) Has(key []byte) (bool, error)   { return d.visible.Has(key) }
func (d *syncVisibilityDB) Iterator(start, end []byte) (corestore.Iterator, error) {
	return d.visible.Iterator(start, end)
}
func (d *syncVisibilityDB) ReverseIterator(start, end []byte) (corestore.Iterator, error) {
	return d.visible.ReverseIterator(start, end)
}
func (d *syncVisibilityDB) Close() error { return nil }
func (d *syncVisibilityDB) NewBatch() corestore.Batch {
	return &syncVisibilityBatch{db: d}
}
func (d *syncVisibilityDB) NewBatchWithSize(size int) corestore.Batch {
	return &syncVisibilityBatch{db: d}
}

type syncVisibilityBatch struct {
	db     *syncVisibilityDB
	ops    []stagedVisibilityOp
	closed bool
}

func (b *syncVisibilityBatch) Set(key, value []byte) error {
	if b.closed {
		return ErrNoImport
	}
	b.ops = append(b.ops, stagedVisibilityOp{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	})
	return nil
}

func (b *syncVisibilityBatch) Delete(key []byte) error {
	if b.closed {
		return ErrNoImport
	}
	b.ops = append(b.ops, stagedVisibilityOp{
		key: append([]byte(nil), key...),
		del: true,
	})
	return nil
}

func (b *syncVisibilityBatch) Write() error {
	return b.apply(b.db.pending)
}

func (b *syncVisibilityBatch) WriteSync() error {
	return b.apply(b.db.visible)
}

func (b *syncVisibilityBatch) apply(target *dbm.MemDB) error {
	if b.closed {
		return ErrNoImport
	}
	for _, op := range b.ops {
		if op.del {
			if err := target.Delete(op.key); err != nil {
				return err
			}
			continue
		}
		if err := target.Set(op.key, op.value); err != nil {
			return err
		}
	}
	b.closed = true
	return nil
}

func (b *syncVisibilityBatch) Close() error {
	b.closed = true
	b.ops = nil
	return nil
}

func (b *syncVisibilityBatch) GetByteSize() (int, error) {
	size := 0
	for _, op := range b.ops {
		size += len(op.key) + len(op.value)
	}
	return size, nil
}

func ExampleImporter() {
	tree := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())

	_, err := tree.Set([]byte("a"), []byte{1})
	if err != nil {
		panic(err)
	}

	_, err = tree.Set([]byte("b"), []byte{2})
	if err != nil {
		panic(err)
	}
	_, err = tree.Set([]byte("c"), []byte{3})
	if err != nil {
		panic(err)
	}
	_, version, err := tree.SaveVersion()
	if err != nil {
		panic(err)
	}

	itree, err := tree.GetImmutable(version)
	if err != nil {
		panic(err)
	}
	exporter, err := itree.Export()
	if err != nil {
		panic(err)
	}
	defer exporter.Close()
	exported := []*ExportNode{}
	for {
		var node *ExportNode
		node, err = exporter.Next()
		if err == ErrorExportDone {
			break
		} else if err != nil {
			panic(err)
		}
		exported = append(exported, node)
	}

	newTree := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())
	importer, err := newTree.Import(version)
	if err != nil {
		panic(err)
	}
	defer importer.Close()
	for _, node := range exported {
		err = importer.Add(node)
		if err != nil {
			panic(err)
		}
	}
	err = importer.Commit()
	if err != nil {
		panic(err)
	}
}

func TestImporter_NegativeVersion(t *testing.T) {
	tree := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())
	_, err := tree.Import(-1)
	require.Error(t, err)
}

func TestImporter_NotEmpty(t *testing.T) {
	tree := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())
	_, err := tree.Set([]byte("a"), []byte{1})
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	_, err = tree.Import(1)
	require.Error(t, err)
}

func TestImporter_NotEmptyDatabase(t *testing.T) {
	db := dbm.NewMemDB()

	tree := NewMutableTree(db, 0, false, NewNopLogger())
	_, err := tree.Set([]byte("a"), []byte{1})
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	tree = NewMutableTree(db, 0, false, NewNopLogger())
	_, err = tree.Load()
	require.NoError(t, err)

	_, err = tree.Import(1)
	require.Error(t, err)
}

func TestImporter_NotEmptyUnsaved(t *testing.T) {
	tree := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())
	_, err := tree.Set([]byte("a"), []byte{1})
	require.NoError(t, err)

	_, err = tree.Import(1)
	require.Error(t, err)
}

func TestImporter_Add(t *testing.T) {
	k := []byte("key")
	v := []byte("value")

	testcases := map[string]struct {
		node  *ExportNode
		valid bool
	}{
		"nil node":          {nil, false},
		"valid":             {&ExportNode{Key: k, Value: v, Version: 1, Height: 0}, true},
		"no key":            {&ExportNode{Key: nil, Value: v, Version: 1, Height: 0}, false},
		"no value":          {&ExportNode{Key: k, Value: nil, Version: 1, Height: 0}, false},
		"version too large": {&ExportNode{Key: k, Value: v, Version: 2, Height: 0}, false},
		"no version":        {&ExportNode{Key: k, Value: v, Version: 0, Height: 0}, false},
		// further cases will be handled by Node.validate()
	}
	for desc, tc := range testcases {
		tc := tc // appease scopelint
		t.Run(desc, func(t *testing.T) {
			tree := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())
			importer, err := tree.Import(1)
			require.NoError(t, err)
			defer importer.Close()

			err = importer.Add(tc.node)
			if tc.valid {
				require.NoError(t, err)
			} else {
				if err == nil {
					err = importer.Commit()
				}
				require.Error(t, err)
			}
		})
	}
}

func TestImporter_Add_Closed(t *testing.T) {
	tree := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())
	importer, err := tree.Import(1)
	require.NoError(t, err)

	importer.Close()
	err = importer.Add(&ExportNode{Key: []byte("key"), Value: []byte("value"), Version: 1, Height: 0})
	require.Error(t, err)
	require.Equal(t, ErrNoImport, err)
}

func TestImporter_Close(t *testing.T) {
	tree := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())
	importer, err := tree.Import(1)
	require.NoError(t, err)

	err = importer.Add(&ExportNode{Key: []byte("key"), Value: []byte("value"), Version: 1, Height: 0})
	require.NoError(t, err)

	importer.Close()
	has, err := tree.Has([]byte("key"))
	require.NoError(t, err)
	require.False(t, has)

	importer.Close()
}

func TestImporter_Commit(t *testing.T) {
	tree := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())
	importer, err := tree.Import(1)
	require.NoError(t, err)

	err = importer.Add(&ExportNode{Key: []byte("key"), Value: []byte("value"), Version: 1, Height: 0})
	require.NoError(t, err)

	err = importer.Commit()
	require.NoError(t, err)
	has, err := tree.Has([]byte("key"))
	require.NoError(t, err)
	require.True(t, has)
}

func TestImporterCommit_CheckpointsBeforeLoadVersionWhenSupported(t *testing.T) {
	db := newStagedVisibilityDB()
	tree := NewMutableTree(db, 0, true, NewNopLogger())
	importer, err := tree.Import(1)
	require.NoError(t, err)
	defer importer.Close()

	require.NoError(t, importer.Add(&ExportNode{
		Key:     []byte("validator"),
		Value:   []byte("present"),
		Version: 1,
		Height:  0,
	}))
	require.NoError(t, importer.Commit())
	require.Equal(t, 1, db.checkpointCount)

	got, err := tree.Get([]byte("validator"))
	require.NoError(t, err)
	require.True(t, bytes.Equal([]byte("present"), got))
}

func TestImporter_Commit_WaitsForInflightBatch(t *testing.T) {
	slowDB := &delayedWriteDB{
		inner:      dbm.NewMemDB(),
		writeDelay: 200 * time.Millisecond,
	}
	tree := NewMutableTree(slowDB, 0, false, NewNopLogger())
	importer, err := tree.Import(1)
	require.NoError(t, err)
	defer importer.Close()

	err = importer.Add(&ExportNode{Key: []byte("key"), Value: []byte("value"), Version: 1, Height: 0})
	require.NoError(t, err)

	// Force Commit()'s root-node write to cross the async flush threshold.
	importer.batchSize = maxBatchSize - 1

	err = importer.Commit()
	require.NoError(t, err)

	has, err := tree.Has([]byte("key"))
	require.NoError(t, err)
	require.True(t, has)
	require.EqualValues(t, 1, tree.Version())
}

func TestImporterCommit_UsesWriteSyncForIntermediateFlushes(t *testing.T) {
	db := newSyncVisibilityDB()
	tree := NewMutableTree(db, 0, true, NewNopLogger())
	importer, err := tree.Import(1)
	require.NoError(t, err)
	defer importer.Close()

	importer.batchSize = maxBatchSize - 1
	require.NoError(t, importer.Add(&ExportNode{
		Key:     []byte("validator"),
		Value:   []byte("present"),
		Version: 1,
		Height:  0,
	}))
	require.NoError(t, importer.Commit())

	got, err := tree.Get([]byte("validator"))
	require.NoError(t, err)
	require.True(t, bytes.Equal([]byte("present"), got))
}

func TestImporter_Commit_ForwardVersion(t *testing.T) {
	tree := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())
	importer, err := tree.Import(2)
	require.NoError(t, err)

	err = importer.Add(&ExportNode{Key: []byte("key"), Value: []byte("value"), Version: 1, Height: 0})
	require.NoError(t, err)

	err = importer.Commit()
	require.NoError(t, err)
	has, err := tree.Has([]byte("key"))
	require.NoError(t, err)
	require.True(t, has)
}

func TestImporter_Commit_Closed(t *testing.T) {
	tree := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())
	importer, err := tree.Import(1)
	require.NoError(t, err)

	err = importer.Add(&ExportNode{Key: []byte("key"), Value: []byte("value"), Version: 1, Height: 0})
	require.NoError(t, err)

	importer.Close()
	err = importer.Commit()
	require.Error(t, err)
	require.Equal(t, ErrNoImport, err)
}

func TestImporter_Commit_Empty(t *testing.T) {
	tree := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())
	importer, err := tree.Import(3)
	require.NoError(t, err)
	defer importer.Close()

	err = importer.Commit()
	require.NoError(t, err)
	assert.EqualValues(t, 3, tree.Version())
}

func BenchmarkImport(b *testing.B) {
	benchmarkImport(b, 4096)
}

func BenchmarkImportBatch(b *testing.B) {
	benchmarkImport(b, maxBatchSize*10)
}

func benchmarkImport(b *testing.B, nodes int) {
	b.StopTimer()
	tree := setupExportTreeSized(b, nodes)
	exported := make([]*ExportNode, 0, nodes)
	exporter, err := tree.Export()
	require.NoError(b, err)
	for {
		item, err := exporter.Next()
		if err == ErrorExportDone {
			break
		} else if err != nil {
			b.Error(err)
		}
		exported = append(exported, item)
	}
	exporter.Close()
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		newTree := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())
		importer, err := newTree.Import(tree.Version())
		require.NoError(b, err)
		for _, item := range exported {
			err = importer.Add(item)
			if err != nil {
				b.Error(err)
			}
		}
		err = importer.Commit()
		require.NoError(b, err)
	}
}
