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
	engine *engine_util.Engines
	conf   *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath

	kvPath := path.Join(dbPath, "kv")
	raftPath := path.Join(dbPath, "raft")

	kvDB := engine_util.CreateDB(kvPath, false)
	//project1没用到raft
	raftDB := engine_util.CreateDB(raftPath, true)

	//func CreateDB(path string, raft bool) *badger.DB
	//func NewEngines(kvEngine, raftEngine *badger.DB, kvPath, raftPath string) *Engines
	return &StandAloneStorage{
		engine: engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath),
		conf:   conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.engine.Kv.Close()

	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	//func (db *DB) NewTransaction(update bool) *Txn
	//For read-only transactions, set update to false.
	txn := s.engine.Kv.NewTransaction(false)
	return NewStandAloneStorageReader(txn), nil
}

type StandAloneStorageReader struct {
	KvTxn *badger.Txn
	// Iter  *engine_util.BadgerIterator
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		KvTxn: txn,
	}
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.KvTxn, cf, key)
	//StandAloneStorage是底层的一个系统,这里认为not found不是err
	//将not found屏蔽掉
	//上层不认为not found是err
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	//func NewCFIterator(cf string, KvTxn *badger.Txn) *BadgerIterator

	return engine_util.NewCFIterator(cf, s.KvTxn)
}

func (s *StandAloneStorageReader) Close() {
	s.KvTxn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	//func PutCF(engine *badger.DB, cf string, key []byte, val []byte)
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			put := b.Data.(storage.Put)
			key := put.Key
			value := put.Value
			cf := put.Cf
			err := engine_util.PutCF(s.engine.Kv, cf, key, value)
			if err != nil {
				return err
			}
			break
		case storage.Delete:
			del := b.Data.(storage.Delete)
			key := del.Key
			cf := del.Cf
			err := engine_util.DeleteCF(s.engine.Kv, cf, key)
			if err != nil {
				return nil
			}
			break
		}
	}
	return nil
}
