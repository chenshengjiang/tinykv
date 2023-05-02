package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if reader != nil {
		defer reader.Close()
	}
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: false,
	}
	if val == nil {
		resp.NotFound = true
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	// 用put初始化一个Modify
	batch := storage.Modify{Data: put}
	err := server.storage.Write(req.Context, []storage.Modify{batch})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	// 和RawPut同理
	delete := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	batch := storage.Modify{Data: delete}
	err := server.storage.Write(req.Context, []storage.Modify{batch})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if reader != nil {
		defer reader.Close()
	}
	if err != nil {
		return nil, err
	}

	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	cnt := 0
	var kvs []*kvrpcpb.KvPair
	// 从startkey开始，返回最多limit个kv，Valid()会判断是否越界了
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if cnt >= int(req.GetLimit()) {
			break
		}
		item := iter.Item()
		key := item.Key()
		val, _ := item.Value()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})
		cnt++
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
