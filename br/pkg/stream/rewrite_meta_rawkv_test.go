// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/consts"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func MockEmptySchemasReplace(midr *mockInsertDeleteRange, dbMap map[UpstreamID]*DBReplace) *SchemasReplace {
	if dbMap == nil {
		dbMap = make(map[UpstreamID]*DBReplace)
	}
	if midr == nil {
		midr = newMockInsertDeleteRange()
	}
	return NewSchemasReplace(
		dbMap,
		nil,
		9527,
		midr.mockRecordDeleteRange,
		false,
	)
}

func produceDBInfoValue(dbName string, dbID int64) ([]byte, error) {
	dbInfo := model.DBInfo{
		ID:   dbID,
		Name: ast.NewCIStr(dbName),
	}
	return json.Marshal(&dbInfo)
}

func produceTableInfoValue(tableName string, tableID int64) ([]byte, error) {
	tableInfo := model.TableInfo{
		ID:   tableID,
		Name: ast.NewCIStr(tableName),
	}

	return json.Marshal(&tableInfo)
}

func TestRewriteKeyForDB(t *testing.T) {
	var (
		dbID   int64  = 1
		dbName        = "db"
		ts     uint64 = 1234
		mDbs          = []byte("DBs")
	)

	encodedKey := utils.EncodeTxnMetaKey(mDbs, meta.DBkey(dbID), ts)

	dbMap := make(map[UpstreamID]*DBReplace)
	downstreamID := dbID + 100
	dbMap[dbID] = NewDBReplace(dbName, downstreamID)

	// create schemasReplace.
	sr := MockEmptySchemasReplace(nil, dbMap)

	// set restoreKV status and rewrite it.
	newKey, err := sr.rewriteKeyForDB(encodedKey, consts.DefaultCF)
	require.Nil(t, err)
	decodedKey, err := ParseTxnMetaKeyFrom(newKey)
	require.Nil(t, err)
	require.Equal(t, decodedKey.Ts, ts)
	newDBID, err := meta.ParseDBKey(decodedKey.Field)
	require.Nil(t, err)
	require.Equal(t, newDBID, downstreamID)

	// rewrite it again, and get the same result.
	newKey, err = sr.rewriteKeyForDB(encodedKey, consts.WriteCF)
	require.Nil(t, err)
	decodedKey, err = ParseTxnMetaKeyFrom(newKey)
	require.Nil(t, err)
	require.Equal(t, decodedKey.Ts, sr.RewriteTS)
	newDBID, err = meta.ParseDBKey(decodedKey.Field)
	require.Nil(t, err)
	require.Equal(t, newDBID, downstreamID)
}

func TestRewriteDBInfo(t *testing.T) {
	var (
		dbID   int64 = 1
		dbName       = "db1"
		DBInfo model.DBInfo
	)

	value, err := produceDBInfoValue(dbName, dbID)
	require.Nil(t, err)

	dbMap := make(map[UpstreamID]*DBReplace)
	dbMap[dbID] = NewDBReplace(dbName, dbID+100)

	// create schemasReplace.
	sr := MockEmptySchemasReplace(nil, dbMap)

	// set restoreKV status and rewrite it.
	newValue, err := sr.rewriteDBInfo(value)
	require.Nil(t, err)
	err = json.Unmarshal(newValue, &DBInfo)
	require.Nil(t, err)
	require.Equal(t, DBInfo.ID, sr.DbReplaceMap[dbID].DbID)

	// rewrite again, and get the same result.
	newId := sr.DbReplaceMap[dbID].DbID
	newValue, err = sr.rewriteDBInfo(value)
	require.Nil(t, err)
	err = json.Unmarshal(newValue, &DBInfo)
	require.Nil(t, err)
	require.Equal(t, DBInfo.ID, sr.DbReplaceMap[dbID].DbID)
	require.Equal(t, newId, sr.DbReplaceMap[dbID].DbID)
}

func TestRewriteKeyForTable(t *testing.T) {
	var (
		dbID      int64  = 1
		dbName           = "db"
		tableID   int64  = 57
		tableName        = "table"
		ts        uint64 = 400036290571534337
	)
	cases := []struct {
		encodeTableFn func(int64) []byte
		decodeTableFn func([]byte) (int64, error)
	}{
		{
			meta.TableKey,
			meta.ParseTableKey,
		},
		{
			meta.AutoIncrementIDKey,
			meta.ParseAutoIncrementIDKey,
		},
		{
			meta.AutoTableIDKey,
			meta.ParseAutoTableIDKey,
		},
		{
			meta.AutoRandomTableIDKey,
			meta.ParseAutoRandomTableIDKey,
		},
		{
			meta.SequenceKey,
			meta.ParseSequenceKey,
		},
	}

	for _, ca := range cases {
		encodedKey := utils.EncodeTxnMetaKey(meta.DBkey(dbID), ca.encodeTableFn(tableID), ts)

		dbMap := make(map[UpstreamID]*DBReplace)
		downStreamDbID := dbID + 100
		dbMap[dbID] = NewDBReplace(dbName, downStreamDbID)
		downStreamTblID := tableID + 100
		dbMap[dbID].TableMap[tableID] = NewTableReplace(tableName, downStreamTblID)

		// create schemasReplace.
		sr := MockEmptySchemasReplace(nil, dbMap)

		// set restoreKV status and rewrite it.
		newKey, err := sr.rewriteKeyForTable(encodedKey, consts.DefaultCF, ca.decodeTableFn, ca.encodeTableFn)
		require.Nil(t, err)
		decodedKey, err := ParseTxnMetaKeyFrom(newKey)
		require.Nil(t, err)
		require.Equal(t, decodedKey.Ts, ts)

		newDbID, err := meta.ParseDBKey(decodedKey.Key)
		require.Nil(t, err)
		require.Equal(t, newDbID, downStreamDbID)
		newTblID, err := ca.decodeTableFn(decodedKey.Field)
		require.Nil(t, err)
		require.Equal(t, newTblID, downStreamTblID)

		// rewrite it again, and get the same result.
		newKey, err = sr.rewriteKeyForTable(encodedKey, consts.WriteCF, ca.decodeTableFn, ca.encodeTableFn)
		require.Nil(t, err)
		decodedKey, err = ParseTxnMetaKeyFrom(newKey)
		require.Nil(t, err)
		require.Equal(t, decodedKey.Ts, sr.RewriteTS)

		newDbID, err = meta.ParseDBKey(decodedKey.Key)
		require.Nil(t, err)
		require.Equal(t, newDbID, downStreamDbID)
		newTblID, err = ca.decodeTableFn(decodedKey.Field)
		require.Nil(t, err)
		require.Equal(t, newTblID, downStreamTblID)
	}
}

func TestRewriteTableInfo(t *testing.T) {
	var (
		dbId      int64 = 40
		dbName          = "db"
		tableID   int64 = 100
		tableName       = "t1"
		tableInfo model.TableInfo
	)

	value, err := produceTableInfoValue(tableName, tableID)
	require.Nil(t, err)

	dbMap := make(map[UpstreamID]*DBReplace)
	dbMap[dbId] = NewDBReplace(dbName, dbId+100)
	dbMap[dbId].TableMap[tableID] = NewTableReplace(tableName, tableID+100)

	// create schemasReplace.
	sr := MockEmptySchemasReplace(nil, dbMap)
	tableCount := 0
	sr.AfterTableRewrittenFn = func(deleted bool, tableInfo *model.TableInfo) {
		tableCount++
		tableInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
			Count: 1,
		}
	}

	// set restoreKV status, rewrite it.
	newValue, err := sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)
	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.ID, sr.DbReplaceMap[dbId].TableMap[tableID].TableID)
	require.EqualValues(t, tableInfo.TiFlashReplica.Count, 1)

	// rewrite it again and get the same result.
	newID := sr.DbReplaceMap[dbId].TableMap[tableID].TableID
	newValue, err = sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)
	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.ID, sr.DbReplaceMap[dbId].TableMap[tableID].TableID)
	require.Equal(t, newID, sr.DbReplaceMap[dbId].TableMap[tableID].TableID)
	require.EqualValues(t, tableCount, 2)
}

func TestRewriteTableInfoForPartitionTable(t *testing.T) {
	var (
		dbId      int64 = 40
		dbName          = "db"
		tableID   int64 = 100
		pt1ID     int64 = 101
		pt2ID     int64 = 102
		tableName       = "t1"
		pt1Name         = "pt1"
		pt2Name         = "pt2"
		tableInfo model.TableInfo
	)

	// create tableinfo.
	pt1 := model.PartitionDefinition{
		ID:   pt1ID,
		Name: ast.NewCIStr(pt1Name),
	}
	pt2 := model.PartitionDefinition{
		ID:   pt2ID,
		Name: ast.NewCIStr(pt2Name),
	}

	pi := model.PartitionInfo{
		Enable:      true,
		Definitions: make([]model.PartitionDefinition, 0),
	}
	pi.Definitions = append(pi.Definitions, pt1)
	pi.Definitions = append(pi.Definitions, pt2)

	tbl := model.TableInfo{
		ID:        tableID,
		Name:      ast.NewCIStr(tableName),
		Partition: &pi,
	}
	value, err := json.Marshal(&tbl)
	require.Nil(t, err)

	dbMap := make(map[UpstreamID]*DBReplace)
	dbMap[dbId] = NewDBReplace(dbName, dbId+100)
	dbMap[dbId].TableMap[tableID] = NewTableReplace(tableName, tableID+100)
	dbMap[dbId].TableMap[tableID].PartitionMap[pt1ID] = pt1ID + 100
	dbMap[dbId].TableMap[tableID].PartitionMap[pt2ID] = pt2ID + 100

	sr := NewSchemasReplace(
		dbMap,
		nil,
		0,
		nil,
		false,
	)

	// set restoreKV status, and rewrite it.
	newValue, err := sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)
	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.String(), tableName)
	require.Equal(t, tableInfo.ID, sr.DbReplaceMap[dbId].TableMap[tableID].TableID)
	require.Equal(
		t,
		tableInfo.Partition.Definitions[0].ID,
		sr.DbReplaceMap[dbId].TableMap[tableID].PartitionMap[pt1ID],
	)
	require.Equal(
		t,
		tbl.Partition.Definitions[0].Name,
		tableInfo.Partition.Definitions[0].Name,
	)
	require.Equal(
		t,
		tableInfo.Partition.Definitions[1].ID,
		sr.DbReplaceMap[dbId].TableMap[tableID].PartitionMap[pt2ID],
	)
	require.Equal(
		t,
		tbl.Partition.Definitions[1].Name,
		tableInfo.Partition.Definitions[1].Name,
	)

	// rewrite it aggin, and get the same result.
	newID1 := sr.DbReplaceMap[dbId].TableMap[tableID].PartitionMap[pt1ID]
	newID2 := sr.DbReplaceMap[dbId].TableMap[tableID].PartitionMap[pt2ID]
	newValue, err = sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)

	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.String(), tableName)
	require.Equal(
		t,
		tableInfo.Partition.Definitions[0].ID,
		sr.DbReplaceMap[dbId].TableMap[tableID].PartitionMap[pt1ID],
	)
	require.Equal(t, tableInfo.Partition.Definitions[0].ID, newID1)
	require.Equal(
		t,
		tableInfo.Partition.Definitions[1].ID,
		sr.DbReplaceMap[dbId].TableMap[tableID].PartitionMap[pt2ID],
	)
	require.Equal(t, tableInfo.Partition.Definitions[1].ID, newID2)
}

func TestRewriteTableInfoForExchangePartition(t *testing.T) {
	var (
		dbID1      int64 = 100
		tableID1   int64 = 101
		pt1ID      int64 = 102
		pt2ID      int64 = 103
		tableName1       = "t1"
		pt1Name          = "pt1"
		pt2Name          = "pt2"

		dbID2      int64 = 105
		tableID2   int64 = 106
		tableName2       = "t2"
		tableInfo  model.TableInfo
		ts         uint64 = 400036290571534337
	)

	// construct table t1 with the partition pi(pt1, pt2).
	pt1 := model.PartitionDefinition{
		ID:   pt1ID,
		Name: ast.NewCIStr(pt1Name),
	}
	pt2 := model.PartitionDefinition{
		ID:   pt2ID,
		Name: ast.NewCIStr(pt2Name),
	}

	pi := model.PartitionInfo{
		Enable:      true,
		Definitions: make([]model.PartitionDefinition, 0),
	}
	pi.Definitions = append(pi.Definitions, pt1, pt2)
	t1 := model.TableInfo{
		ID:        tableID1,
		Name:      ast.NewCIStr(tableName1),
		Partition: &pi,
	}
	db1 := model.DBInfo{}

	// construct table t2 without partition.
	t2 := model.TableInfo{
		ID:   tableID2,
		Name: ast.NewCIStr(tableName2),
	}
	db2 := model.DBInfo{}

	// construct the SchemaReplace
	dbMap := make(map[UpstreamID]*DBReplace)
	dbMap[dbID1] = NewDBReplace(db1.Name.O, dbID1+100)
	dbMap[dbID1].TableMap[tableID1] = NewTableReplace(t1.Name.O, tableID1+100)
	dbMap[dbID1].TableMap[tableID1].PartitionMap[pt1ID] = pt1ID + 100
	dbMap[dbID1].TableMap[tableID1].PartitionMap[pt2ID] = pt2ID + 100

	dbMap[dbID2] = NewDBReplace(db2.Name.O, dbID2+100)
	dbMap[dbID2].TableMap[tableID2] = NewTableReplace(t2.Name.O, tableID2+100)

	tm := NewTableMappingManager()
	tm.MergeBaseDBReplace(dbMap)
	collector := NewMockMetaInfoCollector()

	//exchange partition, t1 partition0 with the t2
	t1Copy := t1.Clone()
	t2Copy := t2.Clone()
	t1Copy.Partition.Definitions[0].ID = tableID2
	t2Copy.ID = pt1ID
	value, err := json.Marshal(&t1Copy)
	require.Nil(t, err)

	// Create an entry for parsing with DefaultCF first
	txnKey := utils.EncodeTxnMetaKey(meta.DBkey(dbID1), meta.TableKey(tableID1), ts)
	defaultCFEntry := &kv.Entry{
		Key:   txnKey,
		Value: value,
	}
	err = tm.ParseMetaKvAndUpdateIdMapping(defaultCFEntry, consts.DefaultCF, ts, collector)
	require.Nil(t, err)

	// Verify that collector is not called for DefaultCF
	require.NotContains(t, collector.tableInfos, dbID1)

	// Now process with WriteCF to make table info visible
	writeCFData := []byte{WriteTypePut}
	writeCFData = codec.EncodeUvarint(writeCFData, ts)
	writeCFEntry := &kv.Entry{
		Key:   txnKey,
		Value: writeCFData,
	}
	err = tm.ParseMetaKvAndUpdateIdMapping(writeCFEntry, consts.WriteCF, ts+1, collector)
	require.Nil(t, err)

	// Verify that collector is now called for WriteCF
	require.Contains(t, collector.tableInfos, dbID1)
	require.Contains(t, collector.tableInfos[dbID1], tableID1)

	sr := NewSchemasReplace(
		tm.DBReplaceMap,
		nil,
		0,
		nil,
		false,
	)

	// rewrite partition table
	value, err = sr.rewriteTableInfo(value, dbID1)
	require.Nil(t, err)
	err = json.Unmarshal(value, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.ID, tableID1+100)
	require.Equal(t, tableInfo.Partition.Definitions[0].ID, tableID2+100)
	require.Equal(t, tableInfo.Partition.Definitions[1].ID, pt2ID+100)

	// rewrite no partition table
	value, err = json.Marshal(&t2Copy)
	require.Nil(t, err)

	// Create an entry for parsing the second table with DefaultCF first
	txnKey = utils.EncodeTxnMetaKey(meta.DBkey(dbID2), meta.TableKey(pt1ID), ts)
	defaultCFEntry2 := &kv.Entry{
		Key:   txnKey,
		Value: value,
	}
	err = tm.ParseMetaKvAndUpdateIdMapping(defaultCFEntry2, consts.DefaultCF, ts, collector)
	require.Nil(t, err)

	// Verify that collector is not called for DefaultCF for the second table
	require.NotContains(t, collector.tableInfos[dbID2], pt1ID)

	// Now process with WriteCF for the second table
	writeCFData2 := []byte{WriteTypePut}
	writeCFData2 = codec.EncodeUvarint(writeCFData2, ts)
	writeCFEntry2 := &kv.Entry{
		Key:   txnKey,
		Value: writeCFData2,
	}
	err = tm.ParseMetaKvAndUpdateIdMapping(writeCFEntry2, consts.WriteCF, ts+1, collector)
	require.Nil(t, err)

	// Verify that collector is now called for WriteCF for the second table
	require.Contains(t, collector.tableInfos, dbID2)
	require.Contains(t, collector.tableInfos[dbID2], pt1ID)

	value, err = sr.rewriteTableInfo(value, dbID2)
	require.Nil(t, err)
	err = json.Unmarshal(value, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.ID, pt1ID+100)
}

func TestRewriteTableInfoForTTLTable(t *testing.T) {
	var (
		dbId      int64 = 40
		dbName          = "db"
		tableID   int64 = 100
		colID     int64 = 1000
		colName         = "t"
		tableName       = "t1"
		tableInfo model.TableInfo
	)

	tbl := model.TableInfo{
		ID:   tableID,
		Name: ast.NewCIStr(tableName),
		Columns: []*model.ColumnInfo{
			{
				ID:        colID,
				Name:      ast.NewCIStr(colName),
				FieldType: *types.NewFieldType(mysql.TypeTimestamp),
			},
		},
		TTLInfo: &model.TTLInfo{
			ColumnName:       ast.NewCIStr(colName),
			IntervalExprStr:  "1",
			IntervalTimeUnit: int(ast.TimeUnitDay),
			Enable:           true,
		},
	}
	value, err := json.Marshal(&tbl)
	require.Nil(t, err)

	dbMap := make(map[UpstreamID]*DBReplace)
	dbMap[dbId] = NewDBReplace(dbName, dbId+100)
	dbMap[dbId].TableMap[tableID] = NewTableReplace(tableName, tableID+100)

	// create empty schemasReplace
	sr := MockEmptySchemasReplace(nil, dbMap)

	// set restoreKV status and rewrite it.
	newValue, err := sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)

	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.String(), tableName)
	require.Equal(t, tableInfo.ID, sr.DbReplaceMap[dbId].TableMap[tableID].TableID)
	require.NotNil(t, tableInfo.TTLInfo)
	require.Equal(t, colName, tableInfo.TTLInfo.ColumnName.O)
	require.Equal(t, "1", tableInfo.TTLInfo.IntervalExprStr)
	require.Equal(t, int(ast.TimeUnitDay), tableInfo.TTLInfo.IntervalTimeUnit)
	require.False(t, tableInfo.TTLInfo.Enable)
}

// db:70->80 -
//           | - t0:71->81 -
//           |             | - p0:72->82
//           |             | - p1:73->83
//           |             | - p2:74->84
//           | - t1:75->85

const (
	mDDLJobDBOldID int64 = 70 + iota
	mDDLJobTable0OldID
	mDDLJobPartition0OldID
	mDDLJobPartition1OldID
	mDDLJobPartition2OldID
	mDDLJobTable1OldID
)

const (
	mDDLJobDBNewID int64 = 80 + iota
	mDDLJobTable0NewID
	mDDLJobPartition0NewID
	mDDLJobPartition1NewID
	mDDLJobPartition2NewID
	mDDLJobTable1NewID
)

var (
	mDDLJobALLNewTableIDSet = map[int64]struct{}{
		mDDLJobTable0NewID:     {},
		mDDLJobPartition0NewID: {},
		mDDLJobPartition1NewID: {},
		mDDLJobPartition2NewID: {},
		mDDLJobTable1NewID:     {},
	}
	mDDLJobALLNewTableKeySet = map[string]struct{}{
		encodeTableKey(mDDLJobTable0NewID):     {},
		encodeTableKey(mDDLJobPartition0NewID): {},
		encodeTableKey(mDDLJobPartition1NewID): {},
		encodeTableKey(mDDLJobPartition2NewID): {},
		encodeTableKey(mDDLJobTable1NewID):     {},
	}
	mDDLJobALLNewPartitionIDSet = map[int64]struct{}{
		mDDLJobPartition0NewID: {},
		mDDLJobPartition1NewID: {},
		mDDLJobPartition2NewID: {},
	}
	mDDLJobALLNewPartitionKeySet = map[string]struct{}{
		encodeTableKey(mDDLJobPartition0NewID): {},
		encodeTableKey(mDDLJobPartition1NewID): {},
		encodeTableKey(mDDLJobPartition2NewID): {},
	}
	mDDLJobALLNewPartitionIndex2KeySet = map[string]struct{}{
		encodeTableIndexKey(mDDLJobPartition0NewID, 2): {},
		encodeTableIndexKey(mDDLJobPartition1NewID, 2): {},
		encodeTableIndexKey(mDDLJobPartition2NewID, 2): {},
	}
	mDDLJobALLNewPartitionIndex3KeySet = map[string]struct{}{
		encodeTableIndexKey(mDDLJobPartition0NewID, 3): {},
		encodeTableIndexKey(mDDLJobPartition1NewID, 3): {},
		encodeTableIndexKey(mDDLJobPartition2NewID, 3): {},
	}
	tempIndex2                             = tablecodec.TempIndexPrefix | int64(2)
	mDDLJobALLNewPartitionTempIndex2KeySet = map[string]struct{}{
		encodeTableIndexKey(mDDLJobPartition0NewID, tempIndex2): {},
		encodeTableIndexKey(mDDLJobPartition1NewID, tempIndex2): {},
		encodeTableIndexKey(mDDLJobPartition2NewID, tempIndex2): {},
	}
	mDDLJobALLIndexesIDSet = map[int64]struct{}{
		2: {},
		3: {},
	}
	mDDLJobAllIndexesKeySet = []map[string]struct{}{
		mDDLJobALLNewPartitionIndex2KeySet, mDDLJobALLNewPartitionIndex3KeySet,
	}
)

var (
	dropSchemaJob                 *model.Job
	dropTable0Job                 *model.Job
	dropTable1Job                 *model.Job
	dropTable0Partition1Job       *model.Job
	reorganizeTable0Partition1Job *model.Job
	removeTable0Partition1Job     *model.Job
	alterTable0Partition1Job      *model.Job
	rollBackTable0IndexJob        = &model.Job{Version: model.JobVersion1, Type: model.ActionAddIndex, State: model.JobStateRollbackDone, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[2,false,[72,73,74]]`)}
	rollBackTable1IndexJob        = &model.Job{Version: model.JobVersion1, Type: model.ActionAddIndex, State: model.JobStateRollbackDone, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[2,false,[]]`)}
	addTable0IndexJob             = &model.Job{Version: model.JobVersion1, Type: model.ActionAddIndex, State: model.JobStateSynced, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[2,false,[72,73,74]]`)}
	addTable1IndexJob             = &model.Job{Version: model.JobVersion1, Type: model.ActionAddIndex, State: model.JobStateSynced, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[2,false,[]]`)}
	dropTable0IndexJob            = &model.Job{Version: model.JobVersion1, Type: model.ActionDropIndex, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`["",false,2,[72,73,74]]`)}
	dropTable1IndexJob            = &model.Job{Version: model.JobVersion1, Type: model.ActionDropIndex, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`["",false,2,[]]`)}
	dropTable0ColumnJob           = &model.Job{Version: model.JobVersion1, Type: model.ActionDropColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`["",false,[2,3],[72,73,74]]`)}
	dropTable1ColumnJob           = &model.Job{Version: model.JobVersion1, Type: model.ActionDropColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`["",false,[2,3],[]]`)}
	modifyTable0ColumnJob         = &model.Job{Version: model.JobVersion1, Type: model.ActionModifyColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[[2,3],[72,73,74]]`)}
	modifyTable1ColumnJob         = &model.Job{Version: model.JobVersion1, Type: model.ActionModifyColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[[2,3],[]]`)}
	multiSchemaChangeJob0         = &model.Job{
		Version:  model.JobVersion1,
		Type:     model.ActionMultiSchemaChange,
		SchemaID: mDDLJobDBOldID,
		TableID:  mDDLJobTable0OldID,
		MultiSchemaInfo: &model.MultiSchemaInfo{
			SubJobs: []*model.SubJob{
				{
					Type:    model.ActionDropIndex,
					RawArgs: json.RawMessage(`[{"O":"k1","L":"k1"},false,2,[72,73,74]]`),
				},
				{
					Type:    model.ActionDropIndex,
					RawArgs: json.RawMessage(`[{"O":"k2","L":"k2"},false,3,[72,73,74]]`),
				},
			},
		},
	}
	multiSchemaChangeJob1 = &model.Job{
		Version:  model.JobVersion1,
		Type:     model.ActionMultiSchemaChange,
		SchemaID: mDDLJobDBOldID,
		TableID:  mDDLJobTable1OldID,
		MultiSchemaInfo: &model.MultiSchemaInfo{
			SubJobs: []*model.SubJob{
				{
					Type:    model.ActionDropIndex,
					RawArgs: json.RawMessage(`[{"O":"k1","L":"k1"},false,2,[]]`),
				},
				{
					Type:    model.ActionDropIndex,
					RawArgs: json.RawMessage(`[{"O":"k2","L":"k2"},false,3,[]]`),
				},
			},
		},
	}
)

func genFinishedJob(job *model.Job, args model.FinishedJobArgs) *model.Job {
	job.FillFinishedArgs(args)
	bytes, _ := job.Encode(true)
	resJob := &model.Job{}
	_ = resJob.Decode(bytes)
	return resJob
}

func init() {
	dropSchemaJob = genFinishedJob(&model.Job{Version: model.GetJobVerInUse(), Type: model.ActionDropSchema,
		SchemaID: mDDLJobDBOldID}, &model.DropSchemaArgs{AllDroppedTableIDs: []int64{71, 72, 73, 74, 75}})
	alterTable0Partition1Job = genFinishedJob(&model.Job{Version: model.GetJobVerInUse(), Type: model.ActionAlterTablePartitioning,
		SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID}, &model.TablePartitionArgs{OldPhysicalTblIDs: []int64{73}})
	removeTable0Partition1Job = genFinishedJob(&model.Job{Version: model.GetJobVerInUse(), Type: model.ActionRemovePartitioning,
		SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID}, &model.TablePartitionArgs{OldPhysicalTblIDs: []int64{73}})
	reorganizeTable0Partition1Job = genFinishedJob(&model.Job{Version: model.GetJobVerInUse(), Type: model.ActionReorganizePartition,
		SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID}, &model.TablePartitionArgs{OldPhysicalTblIDs: []int64{73}})
	dropTable0Partition1Job = genFinishedJob(&model.Job{Version: model.GetJobVerInUse(), Type: model.ActionDropTablePartition,
		SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID}, &model.TablePartitionArgs{OldPhysicalTblIDs: []int64{73}})
	dropTable0Job = genFinishedJob(&model.Job{Version: model.GetJobVerInUse(), Type: model.ActionDropTable,
		SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID}, &model.DropTableArgs{OldPartitionIDs: []int64{72, 73, 74}})
	dropTable1Job = genFinishedJob(&model.Job{Version: model.GetJobVerInUse(), Type: model.ActionDropTable,
		SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID}, &model.DropTableArgs{})
}

type mockInsertDeleteRange struct {
	queryCh chan *PreDelRangeQuery
}

func newMockInsertDeleteRange() *mockInsertDeleteRange {
	// Since there is only single thread, we need to set the channel buf large enough.
	return &mockInsertDeleteRange{
		queryCh: make(chan *PreDelRangeQuery, 10),
	}
}

func (midr *mockInsertDeleteRange) mockRecordDeleteRange(query *PreDelRangeQuery) {
	midr.queryCh <- query
}

func encodeTableKey(tableID int64) string {
	key := tablecodec.EncodeTablePrefix(tableID)
	return hex.EncodeToString(key)
}

func encodeTableIndexKey(tableID, indexID int64) string {
	key := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
	return hex.EncodeToString(key)
}

func TestDeleteRangeForMDDLJob(t *testing.T) {
	midr := newMockInsertDeleteRange()
	partitionMap := map[int64]int64{
		mDDLJobPartition0OldID: mDDLJobPartition0NewID,
		mDDLJobPartition1OldID: mDDLJobPartition1NewID,
		mDDLJobPartition2OldID: mDDLJobPartition2NewID,
	}
	tableReplace0 := &TableReplace{
		TableID:      mDDLJobTable0NewID,
		PartitionMap: partitionMap,
	}
	tableReplace1 := &TableReplace{
		TableID: mDDLJobTable1NewID,
	}
	tableMap := map[int64]*TableReplace{
		mDDLJobTable0OldID: tableReplace0,
		mDDLJobTable1OldID: tableReplace1,
	}
	dbReplace := &DBReplace{
		DbID:     mDDLJobDBNewID,
		TableMap: tableMap,
	}
	schemaReplace := MockEmptySchemasReplace(midr, map[int64]*DBReplace{
		mDDLJobDBOldID: dbReplace,
	})

	var qargs *PreDelRangeQuery
	// drop schema
	err := schemaReplace.processIngestIndexAndDeleteRangeFromJob(dropSchemaJob)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), len(mDDLJobALLNewTableIDSet))
	for _, params := range qargs.ParamsList {
		_, exist := mDDLJobALLNewTableKeySet[params.StartKey]
		require.True(t, exist)
	}

	// drop table0
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(dropTable0Job)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), len(mDDLJobALLNewPartitionIDSet))
	for _, params := range qargs.ParamsList {
		_, exist := mDDLJobALLNewPartitionKeySet[params.StartKey]
		require.True(t, exist)
	}
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, qargs.ParamsList[0].StartKey, encodeTableKey(mDDLJobTable0NewID))

	// drop table1
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(dropTable1Job)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, qargs.ParamsList[0].StartKey, encodeTableKey(mDDLJobTable1NewID))

	// drop table partition1
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(dropTable0Partition1Job)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, qargs.ParamsList[0].StartKey, encodeTableKey(mDDLJobPartition1NewID))

	// reorganize table partition1
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(reorganizeTable0Partition1Job)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, encodeTableKey(mDDLJobPartition1NewID), qargs.ParamsList[0].StartKey)

	// remove table partition1
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(removeTable0Partition1Job)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, encodeTableKey(mDDLJobPartition1NewID), qargs.ParamsList[0].StartKey)

	// alter table partition1
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(alterTable0Partition1Job)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, encodeTableKey(mDDLJobPartition1NewID), qargs.ParamsList[0].StartKey)

	// roll back add index for table0
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(rollBackTable0IndexJob)
	require.NoError(t, err)
	oldPartitionIDMap := make(map[string]struct{})
	for range len(mDDLJobALLNewPartitionIDSet) {
		qargs = <-midr.queryCh
		require.Equal(t, len(qargs.ParamsList), 2)
		for _, params := range qargs.ParamsList {
			_, exist := oldPartitionIDMap[params.StartKey]
			require.False(t, exist)
			oldPartitionIDMap[params.StartKey] = struct{}{}
		}

		// index ID
		_, exist := mDDLJobALLNewPartitionIndex2KeySet[qargs.ParamsList[0].StartKey]
		require.True(t, exist)
		// temp index ID
		_, exist = mDDLJobALLNewPartitionTempIndex2KeySet[qargs.ParamsList[1].StartKey]
		require.True(t, exist)
	}

	// roll back add index for table1
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(rollBackTable1IndexJob)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 2)
	// index ID
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(2)), qargs.ParamsList[0].StartKey)
	// temp index ID
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(tablecodec.TempIndexPrefix|2)), qargs.ParamsList[1].StartKey)

	// drop index for table0
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(dropTable0IndexJob)
	require.NoError(t, err)
	oldPartitionIDMap = make(map[string]struct{})
	for range len(mDDLJobALLNewPartitionIDSet) {
		qargs = <-midr.queryCh
		require.Equal(t, len(qargs.ParamsList), 1)
		_, exist := oldPartitionIDMap[qargs.ParamsList[0].StartKey]
		require.False(t, exist)
		oldPartitionIDMap[qargs.ParamsList[0].StartKey] = struct{}{}
		_, exist = mDDLJobALLNewPartitionIndex2KeySet[qargs.ParamsList[0].StartKey]
		require.True(t, exist)
	}

	// drop index for table1
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(dropTable1IndexJob)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(2)), qargs.ParamsList[0].StartKey)

	// add index for table 0
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(addTable0IndexJob)
	require.NoError(t, err)
	oldPartitionIDMap = make(map[string]struct{})
	for range len(mDDLJobALLNewPartitionIDSet) {
		qargs = <-midr.queryCh
		require.Equal(t, len(qargs.ParamsList), 1)
		_, exist := oldPartitionIDMap[qargs.ParamsList[0].StartKey]
		require.False(t, exist)
		oldPartitionIDMap[qargs.ParamsList[0].StartKey] = struct{}{}
		_, exist = mDDLJobALLNewPartitionTempIndex2KeySet[qargs.ParamsList[0].StartKey]
		require.True(t, exist)
	}

	// add index for table 1
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(addTable1IndexJob)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, tempIndex2), qargs.ParamsList[0].StartKey)

	// drop column for table0
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(dropTable0ColumnJob)
	require.NoError(t, err)
	oldPartitionIDMap = make(map[string]struct{})
	for range len(mDDLJobALLNewPartitionIDSet) {
		qargs = <-midr.queryCh
		require.Equal(t, len(qargs.ParamsList), 2)
		for _, params := range qargs.ParamsList {
			_, exist := oldPartitionIDMap[params.StartKey]
			require.False(t, exist)
			oldPartitionIDMap[params.StartKey] = struct{}{}
		}

		// index ID 2
		_, exist := mDDLJobALLNewPartitionIndex2KeySet[qargs.ParamsList[0].StartKey]
		require.True(t, exist)
		// index ID 3
		_, exist = mDDLJobALLNewPartitionIndex3KeySet[qargs.ParamsList[1].StartKey]
		require.True(t, exist)
	}

	// drop column for table1
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(dropTable1ColumnJob)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), len(mDDLJobALLIndexesIDSet))
	// index ID 2
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(2)), qargs.ParamsList[0].StartKey)
	// index ID 3
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(3)), qargs.ParamsList[1].StartKey)

	// modify column for table0
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(modifyTable0ColumnJob)
	require.NoError(t, err)
	oldPartitionIDMap = make(map[string]struct{})
	for range len(mDDLJobALLNewPartitionIDSet) {
		qargs = <-midr.queryCh
		require.Equal(t, len(qargs.ParamsList), 2)
		for _, params := range qargs.ParamsList {
			_, exist := oldPartitionIDMap[params.StartKey]
			require.False(t, exist)
			oldPartitionIDMap[params.StartKey] = struct{}{}
		}

		// index ID 2
		_, exist := mDDLJobALLNewPartitionIndex2KeySet[qargs.ParamsList[0].StartKey]
		require.True(t, exist)
		// index ID 3
		_, exist = mDDLJobALLNewPartitionIndex3KeySet[qargs.ParamsList[1].StartKey]
		require.True(t, exist)
	}

	// modify column for table1
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(modifyTable1ColumnJob)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), len(mDDLJobALLIndexesIDSet))
	// index ID 2
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(2)), qargs.ParamsList[0].StartKey)
	// index ID 3
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(3)), qargs.ParamsList[1].StartKey)

	// drop indexes(multi-schema-change) for table0
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(multiSchemaChangeJob0)
	require.NoError(t, err)
	oldPartitionIDMap = make(map[string]struct{})
	for l := range 2 {
		for range len(mDDLJobALLNewPartitionIDSet) {
			qargs = <-midr.queryCh
			require.Equal(t, len(qargs.ParamsList), 1)
			_, exist := oldPartitionIDMap[qargs.ParamsList[0].StartKey]
			require.False(t, exist)
			oldPartitionIDMap[qargs.ParamsList[0].StartKey] = struct{}{}
			_, exist = mDDLJobAllIndexesKeySet[l][qargs.ParamsList[0].StartKey]
			require.True(t, exist)
		}
	}

	// drop indexes(multi-schema-change) for table1
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(multiSchemaChangeJob1)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(2)), qargs.ParamsList[0].StartKey)

	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(3)), qargs.ParamsList[0].StartKey)
}

func TestDeleteRangeForMDDLJob2(t *testing.T) {
	midr := newMockInsertDeleteRange()
	partitionMap := map[int64]int64{
		mDDLJobPartition0OldID: mDDLJobPartition0NewID,
		mDDLJobPartition1OldID: mDDLJobPartition1NewID,
		mDDLJobPartition2OldID: mDDLJobPartition2NewID,
	}
	tableReplace0 := &TableReplace{
		TableID:      mDDLJobTable0NewID,
		PartitionMap: partitionMap,
	}
	tableReplace1 := &TableReplace{
		TableID: mDDLJobTable1NewID,
	}
	tableMap := map[int64]*TableReplace{
		mDDLJobTable0OldID: tableReplace0,
		mDDLJobTable1OldID: tableReplace1,
	}
	dbReplace := &DBReplace{
		DbID:     mDDLJobDBNewID,
		TableMap: tableMap,
	}
	schemaReplace := MockEmptySchemasReplace(midr, map[int64]*DBReplace{
		mDDLJobDBOldID: dbReplace,
	})
	var qargs *PreDelRangeQuery
	// drop schema
	err := schemaReplace.processIngestIndexAndDeleteRangeFromJob(dropSchemaJob)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), len(mDDLJobALLNewTableIDSet))
	for _, params := range qargs.ParamsList {
		_, exist := mDDLJobALLNewTableKeySet[params.StartKey]
		require.True(t, exist)
	}
	require.Equal(t, "INSERT IGNORE INTO mysql.gc_delete_range VALUES (%?, %?, %?, %?, %?),(%?, %?, %?, %?, %?),(%?, %?, %?, %?, %?),(%?, %?, %?, %?, %?),(%?, %?, %?, %?, %?)", qargs.Sql)

	// drop schema - lose rewrite rule of table 1
	tableMap_incomplete := map[int64]*TableReplace{
		mDDLJobTable0OldID: tableReplace0,
	}
	dbReplace.TableMap = tableMap_incomplete
	schemaReplace = MockEmptySchemasReplace(midr, map[int64]*DBReplace{
		mDDLJobDBOldID: dbReplace,
	})
	err = schemaReplace.processIngestIndexAndDeleteRangeFromJob(dropSchemaJob)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), len(mDDLJobALLNewPartitionIDSet)+1)
	for _, params := range qargs.ParamsList {
		_, exist := mDDLJobALLNewTableKeySet[params.StartKey]
		require.True(t, exist)
	}
	require.Equal(t, "INSERT IGNORE INTO mysql.gc_delete_range VALUES (%?, %?, %?, %?, %?),(%?, %?, %?, %?, %?),(%?, %?, %?, %?, %?),(%?, %?, %?, %?, %?),", qargs.Sql)
}

func TestCompatibleAlert(t *testing.T) {
	require.Equal(t, ddl.BRInsertDeleteRangeSQLPrefix, `INSERT IGNORE INTO mysql.gc_delete_range VALUES `)
	require.Equal(t, ddl.BRInsertDeleteRangeSQLValue, `(%?, %?, %?, %?, %?)`)
}
