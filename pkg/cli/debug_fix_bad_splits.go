// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugFixBadSplitsCmd = &cobra.Command{
	Use:   "fix-bad-splits",
	Short: "Fix ranges that end before the end of a SQL row by merging it with its adjacent range.",
	Long: `
A previous bug in the backup/restore code can produces ranges that are split at invalid boundaries.

This command fixes such incorrect boundaries by merging to offending
ranges with the range on its right-hand side.
`,
	RunE: clierrorplus.MaybeDecorateError(runDebugFixBadSplits),
}

func runDebugFixBadSplits(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqlConn, err := makeSQLClient("cockroach debug fix-bad-splits", useSystemDb)
	if err != nil {
		return errors.Wrap(err, "could not establish connection to cluster")
	}
	defer sqlConn.Close()

	badRangesQuery := `SELECT range_id, start_key, end_key FROM crdb_internal.ranges`
	type rangeToMerge struct {
		key     roachpb.Key
		endKey  roachpb.Key
		rangeID roachpb.RangeID
	}
	var rangesToMerge []rangeToMerge
	if rows, err := sqlConn.Query(ctx, badRangesQuery); err == nil {
		for {
			vals := make([]driver.Value, 3)
			if err := rows.Next(vals); err == io.EOF {
				break
			} else if err != nil {
				return err
			}
			if rangeID, ok := vals[0].(int64); !ok {
				return errors.New("failed to parse range_id")
			} else {
				if endKey, ok := vals[2].([]byte); !ok {
					return errors.New("failed to parse end_key")
				} else {
					colFamilyLen, err := decodeColumnFamilyLength(endKey)
					if err != nil {
						continue
					}
					if colFamilyLen > 0 {
						if startKey, ok := vals[1].([]byte); !ok {
							return errors.New("failed to parse start_key")
						} else {
							rangesToMerge = append(rangesToMerge, rangeToMerge{
								rangeID: roachpb.RangeID(rangeID),
								key:     startKey,
								endKey:  endKey,
							})
						}
					}
				}
			}
		}
	} else {
		panic(err)
	}

	if len(rangesToMerge) == 0 {
		fmt.Printf("No row splits found\n")
		return nil
	} else {
		fmt.Println("found ranges with end_key splitting SQL row:")
		for _, r := range rangesToMerge {
			fmt.Printf("    RangeID: %d; Start Key: %s; End Key: %s\n", r.rangeID, r.key, r.endKey)
		}
	}

	cc, _, finish, err := getClientGRPCConn(ctx, serverCfg)
	if err != nil {
		log.Errorf(ctx, "connection to server failed: %v", err)
		return err
	}
	defer finish()
	admin := serverpb.NewAdminClient(cc)
	for _, toMerge := range rangesToMerge {
		fmt.Printf("sending AdminMerge request for range %d\n", toMerge.rangeID)
		_, err = admin.SendKVBatch(ctx, &roachpb.BatchRequest{
			Requests: []roachpb.RequestUnion{
				{
					&roachpb.RequestUnion_AdminMerge{
						AdminMerge: &roachpb.AdminMergeRequest{
							RequestHeader: roachpb.RequestHeader{
								Key: toMerge.key,
							},
						},
					},
				},
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// decodeColumnFamily returns the length of the column family of the given key.
// It returns an error if they key isn't a valid table key.
//
// See keys.GrowRowPrefixLength
func decodeColumnFamilyLength(key roachpb.Key) (int, error) {
	// Strip tenant ID prefix to get a "SQL key" starting with a table ID.
	tableKey, _, err := keys.SystemSQLCodec.DecodeTablePrefix(key)
	if err != nil {
		return 0, errors.Errorf("%s: not a valid table key", key)
	}

	if encoding.PeekType(tableKey) != encoding.Int {
		return 0, errors.Errorf("%s: not a valid table key", key)
	}
	// The column family ID length is encoded as a varint and we take advantage
	// of the fact that the column family ID itself will be encoded in 0-9 bytes
	// and thus the length of the column family ID data will fit in a single
	// byte.
	colFamIDLenByte := tableKey[len(tableKey)-1:]
	if encoding.PeekType(colFamIDLenByte) != encoding.Int {
		// The last byte is not a valid column family ID suffix.
		return 0, errors.Errorf("%s: not a valid table key", key)
	}

	_, colFamilyIDLen, err := encoding.DecodeUvarintAscending(colFamIDLenByte)
	if err != nil {
		return 0, err
	}
	return int(colFamilyIDLen), nil
}
