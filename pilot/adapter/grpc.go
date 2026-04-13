package adapter

import (
	"context"
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
		protoOp := serializeOp(op.ID, op.Kind)
		if protoOp != nil {
			protoOps = append(protoOps, protoOp)
		}
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
		if record.GetPayload() == nil {
			result[record.GetKey()] = nil
			continue
		}
		value := string(record.GetPayload())
		var vid uint64
		if record.GetMeta() != nil {
			vid = record.GetMeta().GetVersionId()
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

func serializeOp(id string, kind reference.OpKind) *proto.Operation {
	switch kind.Type {
	case reference.OpKindPut:
		return &proto.Operation{
			OpId: id,
			Op: &proto.Operation_Put{Put: &proto.PutOp{
				Key:       kind.Key,
				Value:     []byte(kind.Value),
				Ephemeral: kind.Ephemeral,
				IndexName: kind.IndexName,
				IndexKey:  kind.IndexKey,
				Sequence:  kind.Sequence,
				Prefix:    kind.Prefix,
				Delta:     kind.Delta,
			}},
		}
	case reference.OpKindGet:
		return &proto.Operation{
			OpId: id,
			Op: &proto.Operation_Get{Get: &proto.GetOp{
				Key:        kind.Key,
				Comparison: kind.Comparison.String(),
			}},
		}
	case reference.OpKindDelete:
		return &proto.Operation{
			OpId: id,
			Op: &proto.Operation_Delete{Delete: &proto.DeleteOp{
				Key: kind.Key,
			}},
		}
	case reference.OpKindDeleteRange:
		return &proto.Operation{
			OpId: id,
			Op: &proto.Operation_DeleteRange{DeleteRange: &proto.DeleteRangeOp{
				Start: kind.Start,
				End:   kind.End,
			}},
		}
	case reference.OpKindScan:
		return &proto.Operation{
			OpId: id,
			Op: &proto.Operation_Scan{Scan: &proto.ScanOp{
				Start:     kind.Start,
				End:       kind.End,
				IndexName: kind.IndexName,
			}},
		}
	case reference.OpKindList:
		return &proto.Operation{
			OpId: id,
			Op: &proto.Operation_List{List: &proto.ListOp{
				Start:     kind.Start,
				End:       kind.End,
				IndexName: kind.IndexName,
			}},
		}
	case reference.OpKindCas:
		return &proto.Operation{
			OpId: id,
			Op: &proto.Operation_Cas{Cas: &proto.CasOp{
				Key:               kind.Key,
				ExpectedVersionId: kind.ExpectedVersionID,
				NewValue:          []byte(kind.NewValue),
			}},
		}
	case reference.OpKindWatchStart:
		return &proto.Operation{
			OpId: id,
			Op: &proto.Operation_WatchStart{WatchStart: &proto.WatchStartOp{
				Prefix: kind.Prefix,
			}},
		}
	case reference.OpKindSessionRestart:
		return &proto.Operation{
			OpId: id,
			Op:   &proto.Operation_SessionRestart{SessionRestart: &proto.SessionRestartOp{}},
		}
	case reference.OpKindGetNotifications:
		return &proto.Operation{
			OpId: id,
			Op:   &proto.Operation_GetNotifications{GetNotifications: &proto.GetNotificationsOp{}},
		}
	default:
		return &proto.Operation{OpId: id}
	}
}

// extractRecord converts a proto Record into key/value/versionID fields on
// the AdapterOpResult.
func extractRecord(rec *proto.Record, result *reference.AdapterOpResult) {
	if rec == nil {
		return
	}
	k := rec.GetKey()
	result.Key = &k
	if rec.GetPayload() != nil {
		v := string(rec.GetPayload())
		result.Value = &v
	}
	if rec.GetMeta() != nil {
		vid := rec.GetMeta().GetVersionId()
		result.VersionID = &vid
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

	switch {
	case r.GetPut() != nil:
		extractRecord(r.GetPut().GetRecord(), &result)
	case r.GetGet() != nil:
		extractRecord(r.GetGet().GetRecord(), &result)
	case r.GetCas() != nil:
		extractRecord(r.GetCas().GetRecord(), &result)
	case r.GetScan() != nil:
		for _, rec := range r.GetScan().GetRecords() {
			rr := reference.RangeRecord{
				Key: rec.GetKey(),
			}
			if rec.GetPayload() != nil {
				rr.Value = string(rec.GetPayload())
			}
			if rec.GetMeta() != nil {
				rr.VersionID = rec.GetMeta().GetVersionId()
			}
			result.Records = append(result.Records, rr)
		}
	case r.GetList() != nil:
		result.Keys = r.GetList().GetKeys()
	}

	return result
}
