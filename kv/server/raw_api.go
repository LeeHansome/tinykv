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
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawGetResponse{Value: value, NotFound: value == nil}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	if err := server.storage.Write(req.Context, []storage.Modify{
		{Data: storage.Put{Key: req.GetKey(), Value: req.GetValue(), Cf: req.GetCf()}},
	}); err != nil {
		return nil, err
	}
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	if err := server.storage.Write(req.Context, []storage.Modify{
		{Data: storage.Delete{Key: req.GetKey(), Cf: req.GetCf()}},
	}); err != nil {
		return nil, err
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	iterCF := reader.IterCF(req.GetCf())
	iterCF.Seek(req.GetStartKey())
	result := &kvrpcpb.RawScanResponse{}
	for i := uint32(0); i < req.GetLimit() && iterCF.Valid(); i++ {
		item := iterCF.Item()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		result.Kvs = append(result.Kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
		iterCF.Next()
	}
	defer iterCF.Close()
	return result, nil
}
