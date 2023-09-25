package access_test

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/alecthomas/assert/v2"

	"github.com/theotheradamsmith/btemulator/internal/access"
	"github.com/theotheradamsmith/btemulator/internal/build"
	"github.com/theotheradamsmith/btemulator/internal/schema"
)

type testDeviceEntry struct {
	AppK string
	schema.DeviceEntry
}

func insertTestCase(t testing.TB, ctx context.Context, tde testDeviceEntry, tbl *bigtable.Table) error {
	t.Helper()
	mut := bigtable.NewMutation()
	mut.DeleteRow()
	mut.Set(schema.ColumnFamilyDeviceProperties, schema.ColumnAppK, bigtable.Now(), []byte(tde.AppK))
	mainkey := fmt.Sprintf("%s#%s#%s", tde.AID, tde.QID, tde.DID)
	return tbl.Apply(ctx, mainkey, mut)
}

func TestReadAidRow(t *testing.T) {
	ctx := context.Background()
	testClient := build.NewBTClient(ctx, schema.Project, schema.Instance)
	t.Run("retrieve an existing row", func(t *testing.T) {
		key, err := access.ReadAidRow(ctx, testClient.Table, "aid-1")
		assert.NoError(t, err)
		assert.Equal(t, "aid-1#qid-1#did-1", key)
	})
	t.Run("retrieve a row that does not exist", func(t *testing.T) {
		key, err := access.ReadAidRow(ctx, testClient.Table, "aid-3")
		assert.Error(t, err)
		assert.Equal(t, "", key)
	})
	t.Run("retrieve an existing row that contains an AppK", func(t *testing.T) {
		tde := testDeviceEntry{
			AppK: "appk-test",
			DeviceEntry: schema.DeviceEntry{
				AID: "aid-test",
				QID: "qid-test",
				DID: "did-test",
			},
		}
		// Inert test
		err := insertTestCase(t, ctx, tde, testClient.Table)
		assert.NoError(t, err)
		// Verify insertion
		key, err := access.ReadAidRow(ctx, testClient.Table, "aid-test")
		assert.NoError(t, err)
		assert.Equal(t, "aid-test#qid-test#did-test", key)
		ready, _, err := access.AidIsPairedAndUnregistered(ctx, testClient.Table, "aid-test")
		assert.False(t, ready)
		assert.Error(t, err)
		assert.IsError(t, err, access.ErrUnexpectedAppK)

		appk, err := access.GetAppK(ctx, testClient.Table, key)
		assert.NoError(t, err)
		assert.Equal(t, []byte("appk-test"), appk)

	})
	t.Run("GetAppK for entry without AppK", func(t *testing.T) {
		key, err := access.ReadAidRow(ctx, testClient.Table, "aid-1")
		assert.Equal(t, "aid-1#qid-1#did-1", key)
		assert.NoError(t, err)
		_, err = access.GetAppK(ctx, testClient.Table, key)
		fmt.Println(key, err)
		assert.Error(t, err)
		assert.IsError(t, err, access.ErrNoAppK)
	})
	testClient.Close()
}

func TestParseRPKey(t *testing.T) {
	t.Run("parse valid key", func(t *testing.T) {
		key := "aid123#qid123#did123"
		qidDID, err := access.ParseRPKey(key)
		assert.NoError(t, err)
		assert.Equal(t, "qid123#did123", qidDID)
	})
	t.Run("parse invalid key", func(t *testing.T) {
		key := "aid123##"
		qidDID, err := access.ParseRPKey(key)
		assert.Error(t, err)
		assert.Equal(t, "", qidDID)
	})
}

func TestGetAidRow(t *testing.T) {
	ctx := context.Background()
	testClient := build.NewBTClient(ctx, schema.Project, schema.Instance)
	row, err := access.GetAidRow(ctx, testClient.Table, "qid-1")
	assert.NoError(t, err)
	readItems := row["DeviceProperties"]
	for _, item := range readItems {
		assert.Equal(t, "DeviceProperties:CreatedDate", item.Column)
	}
	assert.Equal(t, nil, row)
}
