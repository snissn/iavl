package iavl

import (
	"bytes"
	"math/rand"
	"testing"

	corestore "cosmossdk.io/core/store"
	cosmosdb "github.com/cosmos/cosmos-db"
	"github.com/stretchr/testify/require"

	idbm "github.com/cosmos/iavl/db"
)

type cosmosDBWrapper struct {
	cosmosdb.DB
}

func (db *cosmosDBWrapper) Iterator(start, end []byte) (corestore.Iterator, error) {
	return db.DB.Iterator(start, end)
}

func (db *cosmosDBWrapper) ReverseIterator(start, end []byte) (corestore.Iterator, error) {
	return db.DB.ReverseIterator(start, end)
}

func (db *cosmosDBWrapper) NewBatch() corestore.Batch {
	return db.DB.NewBatch()
}

func (db *cosmosDBWrapper) NewBatchWithSize(size int) corestore.Batch {
	return db.DB.NewBatchWithSize(size)
}

type importerBackendCase struct {
	name    string
	backend cosmosdb.BackendType
	profile string
}

func openImporterBackendDB(t *testing.T, tc importerBackendCase, name, dir string) cosmosdb.DB {
	t.Helper()
	if tc.backend == cosmosdb.TreeDBBackend {
		t.Setenv("TREEDB_OPEN_PROFILE", tc.profile)
		t.Setenv("TREEDB_FORCE_CHECKPOINT_ON_WRITE", "0")
	}
	db, err := cosmosdb.NewDB(name, tc.backend, dir)
	require.NoError(t, err)
	return db
}

func buildExportTreeWithSamples(t *testing.T, treeSize int) (*ImmutableTree, [][]byte, map[string][]byte) {
	t.Helper()

	const (
		randSeed  = 49872768940
		keySize   = 16
		valueSize = 16
	)

	r := rand.New(rand.NewSource(randSeed))
	tree := NewMutableTree(idbm.NewMemDB(), 0, false, NewNopLogger())
	sampleKeys := make([][]byte, 0, 8)
	sampleValues := make(map[string][]byte, 8)

	for i := 0; i < treeSize; i++ {
		key := make([]byte, keySize)
		value := make([]byte, valueSize)
		r.Read(key)
		r.Read(value)
		updated, err := tree.Set(key, value)
		require.NoError(t, err)
		if updated {
			i--
			continue
		}
		if len(sampleKeys) < cap(sampleKeys) {
			keyCopy := append([]byte(nil), key...)
			valueCopy := append([]byte(nil), value...)
			sampleKeys = append(sampleKeys, keyCopy)
			sampleValues[string(keyCopy)] = valueCopy
		}
	}

	_, version, err := tree.SaveVersion()
	require.NoError(t, err)
	itree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	return itree, sampleKeys, sampleValues
}

func exportNodes(t *testing.T, tree *ImmutableTree) []*ExportNode {
	t.Helper()
	exporter, err := tree.Export()
	require.NoError(t, err)
	defer exporter.Close()

	nodes := make([]*ExportNode, 0, 16384)
	for {
		node, err := exporter.Next()
		if err == ErrorExportDone {
			break
		}
		require.NoError(t, err)
		nodes = append(nodes, node)
	}
	return nodes
}

func TestImporterLoadVersionBackendParity(t *testing.T) {
	const importVersion = int64(1)
	const treeSize = maxBatchSize*2 + 257
	sourceTree, sampleKeys, sampleValues := buildExportTreeWithSamples(t, treeSize)
	exported := exportNodes(t, sourceTree)

	cases := []importerBackendCase{
		{name: "goleveldb", backend: cosmosdb.GoLevelDBBackend},
		{name: "treedb_fast", backend: cosmosdb.TreeDBBackend, profile: "fast"},
		{name: "treedb_wal_on_fast", backend: cosmosdb.TreeDBBackend, profile: "wal_on_fast"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			name := "testdb"
			dir := t.TempDir()
			db := openImporterBackendDB(t, tc, name, dir)
			t.Cleanup(func() {
				if db != nil {
					require.NoError(t, db.Close())
				}
			})

			prefix := []byte("s/k:staking/")
			importDB := cosmosdb.NewPrefixDB(db, prefix)
			tree := NewMutableTree(&cosmosDBWrapper{DB: importDB}, 0, false, NewNopLogger())
			importer, err := tree.Import(importVersion)
			require.NoError(t, err)
			for _, node := range exported {
				require.NoError(t, importer.Add(node))
			}
			require.NoError(t, importer.Commit())
			importer.Close()

			fresh := NewMutableTree(&cosmosDBWrapper{DB: cosmosdb.NewPrefixDB(db, prefix)}, 0, false, NewNopLogger())
			loaded, err := fresh.LoadVersion(importVersion)
			require.NoError(t, err)
			require.Equal(t, importVersion, loaded)

			for _, key := range sampleKeys {
				got, err := fresh.Get(key)
				require.NoError(t, err)
				require.True(t, bytes.Equal(sampleValues[string(key)], got), "same-handle key mismatch")
			}

			require.NoError(t, db.Close())
			db = nil
			db = openImporterBackendDB(t, tc, name, dir)

			reopened := NewMutableTree(&cosmosDBWrapper{DB: cosmosdb.NewPrefixDB(db, prefix)}, 0, false, NewNopLogger())
			loaded, err = reopened.LoadVersion(importVersion)
			require.NoError(t, err)
			require.Equal(t, importVersion, loaded)

			for _, key := range sampleKeys {
				got, err := reopened.Get(key)
				require.NoError(t, err)
				require.True(t, bytes.Equal(sampleValues[string(key)], got), "reopen key mismatch")
			}
		})
	}
}
