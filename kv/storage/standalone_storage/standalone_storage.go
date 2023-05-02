package standalone_storage

import (
	"path"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	// 对Engine的进一步封装
	engine *engine_util.Engines
	conf   *config.Config
}

// 用来实现StorageReader的接口
type StandAloneStorageReader struct {
	Kvtxn *badger.Txn
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		Kvtxn: txn,
	}
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.Kvtxn, cf, key)
	// 这里需要屏蔽掉 key not found 错误
	// 因为上层认为这并不是error
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	// func NewCFIterator(cf string, txn *badger.Txn) *BadgerIterator
	return engine_util.NewCFIterator(cf, s.Kvtxn)
	// 返回的BadgerIterator类型其实就相当于DBIterator,因为其实现了DBIterator所有的接口函数
}

func (s *StandAloneStorageReader) Close() {
	s.Kvtxn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbpath := conf.DBPath

	// 每个节点都会有两个badgerdb，Engine里有说明，所以这里要创建两个DB
	kvpath := path.Join(dbpath, "kv")
	kv := engine_util.CreateDB(kvpath, false)

	raftpath := path.Join(dbpath, "raft")
	raft := engine_util.CreateDB(raftpath, true)

	return &StandAloneStorage{
		engine: engine_util.NewEngines(kv, raft, kvpath, raftpath),
		conf:   conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.engine.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) { // 这里要返回一个StorageReader，也就需要实现它相应的接口函数
	// Your Code Here (1).
	// For read-only transactions, set update to false.
	txn := s.engine.Kv.NewTransaction(false)
	return NewStandAloneStorageReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// 需要将每个 Modify 根据类型进行 put or delete
	var err error
	for _, m := range batch {
		cf, key, val := m.Cf(), m.Key(), m.Value()
		switch m.Data.(type) {
		case storage.Put:
			err = engine_util.PutCF(s.engine.Kv, cf, key, val)
		case storage.Delete:
			err = engine_util.DeleteCF(s.engine.Kv, cf, key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
