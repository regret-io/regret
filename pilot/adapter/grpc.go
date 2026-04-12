package adapter

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/regret-io/regret/pilot-go/proto"
	"github.com/regret-io/regret/pilot-go/reference"
)

// GrpcAdapterClient implements engine.AdapterClient via gRPC.
type GrpcAdapterClient struct {
	client proto.AdapterServiceClient
	conn   *grpc.ClientConn
}

// Connect dials the adapter gRPC server and returns a client.
func Connect(addr string) (*GrpcAdapterClient, error) {
	url := addr
	if len(url) > 0 && url[0] != 'h' {
		// no scheme
		url = addr
	} else {
		// strip http:// for grpc dial
		for _, prefix := range []string{"http://", "https://"} {
			if len(url) > len(prefix) && url[:len(prefix)] == prefix {
				url = url[len(prefix):]
				break
			}
		}
	}

	conn, err := grpc.NewClient(url,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", url, err)
	}
	client := proto.NewAdapterServiceClient(conn)
	return &GrpcAdapterClient{client: client, conn: conn}, nil
}

// Close closes the underlying gRPC connection.
func (c *GrpcAdapterClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ExecuteBatch sends a batch of operations to the adapter.
func (c *GrpcAdapterClient) ExecuteBatch(ctx context.Context, batchID string, ops []reference.Operation) ([]reference.AdapterOpResult, error) {
	protoOps := make([]*proto.Operation, 0, len(ops))
	for _, op := range ops {
		if op.ID == "" {
			continue
		}
		opType, payload := serializeOp(op.Kind)
		protoOps = append(protoOps, &proto.Operation{
			OpId:    op.ID,
			OpType:  opType,
			Payload: payload,
		})
	}

	resp, err := c.client.ExecuteBatch(ctx, &proto.BatchRequest{
		BatchId: batchID,
		Ops:     protoOps,
	})
	if err != nil {
		return nil, fmt.Errorf("ExecuteBatch: %w", err)
	}

	results := make([]reference.AdapterOpResult, len(resp.GetResults()))
	for i, r := range resp.GetResults() {
		results[i] = parseOpResult(r)
	}
	return results, nil
}

// ReadState reads all adapter state under the given key prefix.
func (c *GrpcAdapterClient) ReadState(ctx context.Context, keyPrefix string) (map[string]*reference.RecordState, error) {
	resp, err := c.client.ReadState(ctx, &proto.ReadStateRequest{KeyPrefix: keyPrefix})

	if err != nil {
		return nil, fmt.Errorf("ReadState: %w", err)
	}

	result := make(map[string]*reference.RecordState, len(resp.GetRecords()))
	for _, record := range resp.GetRecords() {
		if record.Value == nil {
			result[record.GetKey()] = nil
			continue
		}
		value := string(record.GetValue())
		var vid uint64
		if v, ok := record.GetMetadata()["version_id"]; ok {
			fmt.Sscanf(v, "%d", &vid)
		}
		result[record.GetKey()] = &reference.RecordState{
			Value:     &value,
			VersionID: vid,
		}
	}
	return result, nil
}

// Cleanup deletes all data under the given key prefix.
func (c *GrpcAdapterClient) Cleanup(ctx context.Context, keyPrefix string) error {
	_, err := c.client.Cleanup(ctx, &proto.CleanupRequest{KeyPrefix: keyPrefix})
	if err != nil {
		return fmt.Errorf("Cleanup: %w", err)
	}
	return nil
}

// CleanupPrefix is a convenience function that connects, cleans up, and disconnects.
func CleanupPrefix(addr, keyPrefix string) error {
	client, err := Connect(addr)
	if err != nil {
		return err
	}
	defer client.Close()
	return client.Cleanup(context.Background(), keyPrefix)
}

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

func serializeOp(kind reference.OpKind) (string, []byte) {
	switch kind.Type {
	case reference.OpKindPut:
		return "put", mustJSON(map[string]interface{}{"key": kind.Key, "value": kind.Value})
	case reference.OpKindGet:
		opType := "get"
		switch kind.Comparison {
		case reference.ComparisonFloor:
			opType = "get_floor"
		case reference.ComparisonCeiling:
			opType = "get_ceiling"
		case reference.ComparisonLower:
			opType = "get_lower"
		case reference.ComparisonHigher:
			opType = "get_higher"
		}
		return opType, mustJSON(map[string]interface{}{"key": kind.Key})
	case reference.OpKindDelete:
		return "delete", mustJSON(map[string]interface{}{"key": kind.Key})
	case reference.OpKindDeleteRange:
		return "delete_range", mustJSON(map[string]interface{}{"start": kind.Start, "end": kind.End})
	case reference.OpKindList:
		return "list", mustJSON(map[string]interface{}{"start": kind.Start, "end": kind.End})
	case reference.OpKindRangeScan:
		return "range_scan", mustJSON(map[string]interface{}{"start": kind.Start, "end": kind.End})
	case reference.OpKindCas:
		return "cas", mustJSON(map[string]interface{}{"key": kind.Key, "expected_version_id": kind.ExpectedVersionID, "new_value": kind.NewValue})
	case reference.OpKindEphemeralPut:
		return "ephemeral_put", mustJSON(map[string]interface{}{"key": kind.Key, "value": kind.Value})
	case reference.OpKindIndexedPut:
		return "indexed_put", mustJSON(map[string]interface{}{"key": kind.Key, "value": kind.Value, "index_name": kind.IndexName, "index_key": kind.IndexKey})
	case reference.OpKindIndexedGet:
		return "indexed_get", mustJSON(map[string]interface{}{"index_name": kind.IndexName, "index_key": kind.IndexKey})
	case reference.OpKindIndexedList:
		return "indexed_list", mustJSON(map[string]interface{}{"index_name": kind.IndexName, "start": kind.Start, "end": kind.End})
	case reference.OpKindIndexedRangeScan:
		return "indexed_range_scan", mustJSON(map[string]interface{}{"index_name": kind.IndexName, "start": kind.Start, "end": kind.End})
	case reference.OpKindSequencePut:
		return "sequence_put", mustJSON(map[string]interface{}{"prefix": kind.Prefix, "value": kind.Value, "delta": kind.Delta})
	case reference.OpKindWatchStart:
		return "watch_start", mustJSON(map[string]interface{}{"key": kind.Key})
	case reference.OpKindSessionRestart:
		return "session_restart", nil
	case reference.OpKindGetNotifications:
		return "get_notifications", nil
	case reference.OpKindFence:
		return "fence", nil
	default:
		return "unknown", nil
	}
}

func parseOpResult(r *proto.OpResult) reference.AdapterOpResult {
	result := reference.AdapterOpResult{
		OpID:   r.GetOpId(),
		Status: r.GetStatus(),
	}
	if r.GetMessage() != "" {
		msg := r.GetMessage()
		result.Message = &msg
	}

	if len(r.GetPayload()) == 0 {
		return result
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(r.Payload, &payload); err != nil {
		return result
	}

	if key, ok := payload["key"].(string); ok {
		result.Key = &key
	}
	if value, ok := payload["value"].(string); ok {
		result.Value = &value
	}
	if vid, ok := payload["version_id"].(float64); ok {
		v := uint64(vid)
		result.VersionID = &v
	}
	if records, ok := payload["records"].([]interface{}); ok {
		for _, rec := range records {
			if rm, ok := rec.(map[string]interface{}); ok {
				rr := reference.RangeRecord{}
				if k, ok := rm["key"].(string); ok {
					rr.Key = k
				}
				if v, ok := rm["value"].(string); ok {
					rr.Value = v
				}
				if vid, ok := rm["version_id"].(float64); ok {
					rr.VersionID = uint64(vid)
				}
				result.Records = append(result.Records, rr)
			}
		}
	}
	if keys, ok := payload["keys"].([]interface{}); ok {
		for _, k := range keys {
			if s, ok := k.(string); ok {
				result.Keys = append(result.Keys, s)
			}
		}
	}
	if dc, ok := payload["deleted_count"].(float64); ok {
		v := uint64(dc)
		result.DeletedCount = &v
	}
	if notifs, ok := payload["notifications"]; ok {
		data, err := json.Marshal(notifs)
		if err == nil {
			var nrs []reference.NotificationRecord
			if json.Unmarshal(data, &nrs) == nil {
				result.Notifications = nrs
			}
		}
	}

	return result
}

func mustJSON(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return data
}
