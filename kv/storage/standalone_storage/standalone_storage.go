package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engines *engine_util.Engines
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	snapPath := filepath.Join(dbPath, "snap")

	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)
	os.Mkdir(snapPath, os.ModePerm)

	raftDB := engine_util.CreateDB(raftPath, true)
	kvDB := engine_util.CreateDB(kvPath, false)
	engines := engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)
	return &StandAloneStorage{engines: engines, config: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return standAloneReader{
		inner:     s,
		iterCount: 0,
		txn:       s.engines.Kv.NewTransaction(false),
	}, nil
}

type standAloneReader struct {
	txn       *badger.Txn
	inner     *StandAloneStorage
	iterCount int
}

func (s standAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	cfResp, err := engine_util.GetCF(s.inner.engines.Kv, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return cfResp, nil
}

func (s standAloneReader) IterCF(cf string) engine_util.DBIterator {
	s.iterCount += 1
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s standAloneReader) Close() {
	if s.iterCount > 0 {
		panic("Unclosed iterator")
	}
	s.txn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := &engine_util.WriteBatch{}
	for _, modify := range batch {
		wb.SetCF(modify.Cf(), modify.Key(), modify.Value())
	}
	return s.engines.WriteKV(wb)
}
