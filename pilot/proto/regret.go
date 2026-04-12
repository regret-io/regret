// Package proto provides hand-written stubs for the regret.v1 protobuf service.
// TODO: Regenerate from regret.proto using protoc-gen-go and protoc-gen-go-grpc.
//
//	protoc --go_out=. --go-grpc_out=. regret.proto
package proto

import (
	"context"

	"google.golang.org/grpc"
)

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

// Operation is a single operation in a batch request.
type Operation struct {
	OpID   string `json:"op_id"`
	OpType string `json:"op_type"`
	// Payload is a JSON-encoded byte slice.
	Payload []byte `json:"payload"`
}

// BatchRequest is the request for ExecuteBatch.
type BatchRequest struct {
	BatchID string      `json:"batch_id"`
	Ops     []Operation `json:"ops"`
}

// OpResult is a single operation result.
type OpResult struct {
	OpID    string `json:"op_id"`
	Status  string `json:"status"`
	Payload []byte `json:"payload"`
	Message string `json:"message"`
}

// BatchResponse is the response for ExecuteBatch.
type BatchResponse struct {
	BatchID string     `json:"batch_id"`
	Results []OpResult `json:"results"`
}

// ReadStateRequest is the request for ReadState.
type ReadStateRequest struct {
	KeyPrefix string `json:"key_prefix"`
}

// Record is a single record in a ReadStateResponse.
type Record struct {
	Key      string            `json:"key"`
	Value    []byte            `json:"value,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ReadStateResponse is the response for ReadState.
type ReadStateResponse struct {
	Records []Record `json:"records"`
}

// CleanupRequest is the request for Cleanup.
type CleanupRequest struct {
	KeyPrefix string `json:"key_prefix"`
}

// CleanupResponse is the response for Cleanup.
type CleanupResponse struct{}

// ---------------------------------------------------------------------------
// Client interface
// ---------------------------------------------------------------------------

// AdapterServiceClient is the gRPC client interface for the AdapterService.
type AdapterServiceClient interface {
	ExecuteBatch(ctx context.Context, in *BatchRequest, opts ...grpc.CallOption) (*BatchResponse, error)
	ReadState(ctx context.Context, in *ReadStateRequest, opts ...grpc.CallOption) (*ReadStateResponse, error)
	Cleanup(ctx context.Context, in *CleanupRequest, opts ...grpc.CallOption) (*CleanupResponse, error)
}

// adapterServiceClient implements AdapterServiceClient via gRPC.
type adapterServiceClient struct {
	cc grpc.ClientConnInterface
}

// NewAdapterServiceClient creates a new AdapterServiceClient.
func NewAdapterServiceClient(cc grpc.ClientConnInterface) AdapterServiceClient {
	return &adapterServiceClient{cc: cc}
}

const adapterServiceName = "/regret.v1.AdapterService/"

func (c *adapterServiceClient) ExecuteBatch(ctx context.Context, in *BatchRequest, opts ...grpc.CallOption) (*BatchResponse, error) {
	out := new(BatchResponse)
	err := c.cc.Invoke(ctx, adapterServiceName+"ExecuteBatch", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adapterServiceClient) ReadState(ctx context.Context, in *ReadStateRequest, opts ...grpc.CallOption) (*ReadStateResponse, error) {
	out := new(ReadStateResponse)
	err := c.cc.Invoke(ctx, adapterServiceName+"ReadState", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adapterServiceClient) Cleanup(ctx context.Context, in *CleanupRequest, opts ...grpc.CallOption) (*CleanupResponse, error) {
	out := new(CleanupResponse)
	err := c.cc.Invoke(ctx, adapterServiceName+"Cleanup", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
