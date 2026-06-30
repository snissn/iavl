package iavl

import (
	"errors"
	"testing"

	dbm "github.com/cosmos/iavl/db"
)

type stubViewBatch struct {
	setCount     int
	setViewCount int
	writeErr     error
	writeCount   int
	size         int
}

func (b *stubViewBatch) Set(key, value []byte) error {
	b.setCount++
	b.size += len(key) + len(value)
	return nil
}

func (b *stubViewBatch) SetView(key, value []byte) error {
	b.setViewCount++
	b.size += len(key) + len(value)
	return nil
}

func (b *stubViewBatch) Delete(key []byte) error {
	b.size += len(key)
	return nil
}

func (b *stubViewBatch) Write() error {
	b.writeCount++
	return b.writeErr
}
func (b *stubViewBatch) WriteSync() error { return nil }
func (b *stubViewBatch) Close() error     { return nil }

func (b *stubViewBatch) GetByteSize() (int, error) {
	return b.size, nil
}

var _ dbm.Batch = (*stubViewBatch)(nil)

func TestBatchSetOwned_UsesSetViewWhenAvailable(t *testing.T) {
	stub := &stubViewBatch{}
	if err := batchSetOwned(stub, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("batchSetOwned: %v", err)
	}
	if stub.setViewCount != 1 || stub.setCount != 0 {
		t.Fatalf("expected SetView path, set=%d setview=%d", stub.setCount, stub.setViewCount)
	}
}

func TestBatchWithFlusherSetView_ForwardsWhenAvailable(t *testing.T) {
	stub := &stubViewBatch{}
	b := &BatchWithFlusher{
		batch:          stub,
		flushThreshold: 1 << 20,
	}
	if err := b.SetView([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("setview: %v", err)
	}
	if stub.setViewCount != 1 || stub.setCount != 0 {
		t.Fatalf("expected SetView forwarding, set=%d setview=%d", stub.setCount, stub.setViewCount)
	}
}

func TestBatchWithFlusherReturnsWriteErrorAfterThresholdFlush(t *testing.T) {
	writeErr := errors.New("write failed")
	tests := []struct {
		name string
		op   func(*BatchWithFlusher) error
	}{
		{
			name: "set",
			op: func(b *BatchWithFlusher) error {
				return b.Set([]byte("key"), []byte("value"))
			},
		},
		{
			name: "set-view",
			op: func(b *BatchWithFlusher) error {
				return b.SetView([]byte("key"), []byte("value"))
			},
		},
		{
			name: "delete",
			op: func(b *BatchWithFlusher) error {
				return b.Delete([]byte("key"))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := &stubViewBatch{writeErr: writeErr}
			b := &BatchWithFlusher{
				batch:          stub,
				flushThreshold: 1,
			}
			if err := tt.op(b); !errors.Is(err, writeErr) {
				t.Fatalf("err=%v want %v", err, writeErr)
			}
			if stub.writeCount != 1 {
				t.Fatalf("writeCount=%d want 1", stub.writeCount)
			}
		})
	}
}

type viewCountingDB struct {
	inner        dbm.DB
	setCount     int
	setViewCount int
}

func (db *viewCountingDB) Get(key []byte) ([]byte, error) { return db.inner.Get(key) }
func (db *viewCountingDB) Has(key []byte) (bool, error)   { return db.inner.Has(key) }

func (db *viewCountingDB) Iterator(start, end []byte) (dbm.Iterator, error) {
	return db.inner.Iterator(start, end)
}

func (db *viewCountingDB) ReverseIterator(start, end []byte) (dbm.Iterator, error) {
	return db.inner.ReverseIterator(start, end)
}

func (db *viewCountingDB) Close() error { return db.inner.Close() }

func (db *viewCountingDB) NewBatch() dbm.Batch {
	return &viewCountingBatch{db: db, inner: db.inner.NewBatch()}
}

func (db *viewCountingDB) NewBatchWithSize(size int) dbm.Batch {
	return &viewCountingBatch{db: db, inner: db.inner.NewBatchWithSize(size)}
}

type viewCountingBatch struct {
	db    *viewCountingDB
	inner dbm.Batch
}

func (b *viewCountingBatch) Set(key, value []byte) error {
	b.db.setCount++
	return b.inner.Set(key, value)
}

func (b *viewCountingBatch) SetView(key, value []byte) error {
	b.db.setViewCount++
	return b.inner.Set(key, value)
}

func (b *viewCountingBatch) Delete(key []byte) error {
	return b.inner.Delete(key)
}

func (b *viewCountingBatch) Write() error              { return b.inner.Write() }
func (b *viewCountingBatch) WriteSync() error          { return b.inner.WriteSync() }
func (b *viewCountingBatch) Close() error              { return b.inner.Close() }
func (b *viewCountingBatch) GetByteSize() (int, error) { return b.inner.GetByteSize() }
func TestImporterUsesSetViewForOwnedNodeWrites(t *testing.T) {
	db := &viewCountingDB{inner: dbm.NewMemDB()}
	tree := NewMutableTree(db, 0, false, NewNopLogger())
	importer, err := tree.Import(1)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if err := importer.Add(&ExportNode{Key: []byte("key"), Value: []byte("value"), Version: 1, Height: 0}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := importer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if db.setViewCount == 0 {
		t.Fatalf("importer setViewCount=0, setCount=%d", db.setCount)
	}
	has, err := tree.Has([]byte("key"))
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !has {
		t.Fatal("imported key missing")
	}
}
