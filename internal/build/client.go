package build

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/theotheradamsmith/btemulator/internal/schema"
)

func makeMain(ctx context.Context, tbl *bigtable.Table) []string {
	muts := make([]*bigtable.Mutation, len(schema.Devices))
	rowKeys := make([]string, len(schema.Devices))

	timestamp := bigtable.Now()

	for i, d := range schema.Devices {
		muts[i] = bigtable.NewMutation()
		muts[i].DeleteCellsInFamily(schema.ColumnFamilyFirebaseProperties)
		muts[i].DeleteCellsInFamily(schema.ColumnFamilyDeviceProperties)
		muts[i].Set(schema.ColumnFamilyFirebaseProperties, schema.ColumnFCM, timestamp, []byte(d.FCM))
		muts[i].Set(schema.ColumnFamilyDeviceProperties, schema.ColumnCreated, timestamp, []byte(bigtable.Now().Time().Format(time.UnixDate)))
		muts[i].Set(schema.ColumnFamilyDeviceProperties, schema.ColumnDID, timestamp, []byte(d.DID))
		rowKeys[i] = fmt.Sprintf("%s#%s", d.QID, d.DID)
	}

	log.Println("Applying bulk changes...")

	rowErrs, err := tbl.ApplyBulk(ctx, rowKeys, muts)
	if err != nil {
		log.Fatalf("Could not apply bulk row mutation: %v", err)
	}
	if rowErrs != nil {
		for _, rowErr := range rowErrs {
			log.Printf("Error writing row: %v", rowErr)
		}
		log.Fatalf("Could not write some rows")
	}
	return rowKeys
}

func makeAID(ctx context.Context, tbl *bigtable.Table) []string {
	muts := make([]*bigtable.Mutation, len(schema.Devices))
	rowKeys := make([]string, len(schema.Devices))

	for i, d := range schema.Devices {
		muts[i] = bigtable.NewMutation()
		muts[i].DeleteCellsInFamily(schema.ColumnFamilyDeviceProperties)
		muts[i].Set(schema.ColumnFamilyDeviceProperties, schema.ColumnCreated, bigtable.Now(), []byte(bigtable.Now().Time().Format(time.UnixDate)))
		mainkey := fmt.Sprintf("%s#%s#%s", d.AID, d.QID, d.DID)
		//muts[i].Set(schema.ColumnFamilyDeviceProperties, schema.ColumnAppK, bigtable.Now(), []byte(mainkey))
		rowKeys[i] = mainkey
	}

	log.Println("Applying bulk changes...")

	rowErrs, err := tbl.ApplyBulk(ctx, rowKeys, muts)
	if err != nil {
		log.Fatalf("Could not apply bulk row mutation: %v", err)
	}
	if rowErrs != nil {
		for _, rowErr := range rowErrs {
			log.Printf("Error writing row: %v", rowErr)
		}
		log.Fatalf("Could not write some rows")
	}
	return rowKeys
}

type BTClient struct {
	Client *bigtable.Client
	Table  *bigtable.Table
}

func NewBTClient(ctx context.Context, project, instance string) *BTClient {
	client, err := bigtable.NewClient(ctx, project, instance)
	if err != nil {
		log.Fatalf("Could not create data operations client: %v", err)
	}

	tbl := client.Open(schema.TableName)

	return &BTClient{
		Client: client,
		Table:  tbl,
	}
}

func (b *BTClient) Close() {
	if err := b.Client.Close(); err != nil {
		log.Fatalf("Could not close data operations client: %v", err)
	}

}

func DoClient(ctx context.Context, project, instance string) (*bigtable.Client, *bigtable.Table) {
	client, err := bigtable.NewClient(ctx, project, instance)
	if err != nil {
		log.Fatalf("Could not create data operations client: %v", err)
	}

	tbl := client.Open(schema.TableName)

	//rowKeys := makeMain(ctx, tbl)
	_ = makeMain(ctx, tbl)
	_ = makeAID(ctx, tbl)

	addTestData(ctx)

	/*
		log.Printf("Getting a single greeting by row key:")
		//row, err := tbl.ReadRow(ctx, rowKeys[0], bigtable.RowFilter(bigtable.ColumnFilter(columnDID)))
		row, err := tbl.ReadRow(ctx, rowKeys[0])

		log.Printf("\n\n%s = %v\n\n", rowKeys[0], row)

		if err != nil {
			log.Fatalf("Could not read row with key %s: %v", rowKeys[0], err)
		}
		log.Printf("\t%s = %s\n", rowKeys[0], string(row[schema.ColumnFamilyDeviceProperties][0].Value))
	*/

	return client, tbl
}

type bigtableDataEntry struct {
	columnFamilyName string
	columnName       string
	data             []byte
}

type testEntry struct {
	key        string
	qid        string
	did        string
	properties map[string]bigtableDataEntry
}

const (
	aid        = "aid"
	appk       = "appk"
	trusted    = "trusted"
	registered = "registered"
	challenge  = "challenge"

	aidMcCoy = "wyszz-ty4ey-eqgtc-ae44e-47yjg"
	qidMcCoy = "qid-mccoy"
	didMcCoy = "did-mccoy"

	aidReady = "aid-ready"
	qidReady = "qid-ready"
	didReady = "did-ready"

	aidInFlight  = "aid-in-flight"
	qidInFlight  = "qid-in-flight"
	didInFlight  = "did-in-flight"
	appkInFlight = "appk-in-flight"

	aidRegistered  = "aid-already-registered"
	qidRegistered  = "qid-already-registered"
	didRegistered  = "did-already-registered"
	appkRegistered = "appk-already-registered"

	hardware = "hardware"
	software = "software"
)

/*
var (
	aidCol       = fmt.Sprintf("%s:%s", schema.ColumnFamilyDeviceProperties, schema.ColumnAID)
	appkCol      = fmt.Sprintf("%s:%s", schema.ColumnFamilyDeviceProperties, schema.ColumnAppK)
	trustedCol   = fmt.Sprintf("%s:%s", schema.ColumnFamilyDeviceProperties, schema.ColumnTrusted)
	challengeCol = fmt.Sprintf("%s:%s", schema.ColumnFamilyRegistrationProperties, schema.ColumnChallenge)
	//registeredCol = fmt.Sprintf("%s:%s", ColumnFamilyRegistrationProperties, ColumnRegistered)
)
*/

var (
	theRealMcCoy = testEntry{
		key: fmt.Sprintf("%s#%s", qidMcCoy, didMcCoy),
		qid: qidMcCoy,
		did: didMcCoy,
		properties: map[string]bigtableDataEntry{
			aid: {
				columnFamilyName: schema.ColumnFamilyDeviceProperties,
				columnName:       schema.ColumnAID,
				data:             []byte(aidMcCoy),
			},
		},
	}

	// readyX expects that a valid pairing of aid & did exists in the RP schema,
	// and that the DID hasn't already been associated with a different AID
	readyEntry = testEntry{
		key: fmt.Sprintf("%s#%s", qidReady, didReady),
		qid: qidReady,
		did: didReady,
		properties: map[string]bigtableDataEntry{
			aid: {
				columnFamilyName: schema.ColumnFamilyDeviceProperties,
				columnName:       schema.ColumnAID,
				data:             []byte(aidReady),
			},
		},
	}

	// inFlightX is in the process of being registered; an AppK has been written
	// to the RP entry and new attempts to register should fail
	inFlightEntry = testEntry{
		key: fmt.Sprintf("%s#%s", qidInFlight, didInFlight),
		qid: qidInFlight,
		did: didInFlight,
		properties: map[string]bigtableDataEntry{
			aid: {
				columnFamilyName: schema.ColumnFamilyDeviceProperties,
				columnName:       schema.ColumnAID,
				data:             []byte(aidInFlight),
			},
			appk: {
				columnFamilyName: schema.ColumnFamilyDeviceProperties,
				columnName:       schema.ColumnAppK,
				data:             []byte(appkInFlight),
			},
			trusted: {
				columnFamilyName: schema.ColumnFamilyDeviceProperties,
				columnName:       schema.ColumnTrusted,
				data:             []byte(hardware),
			},
			challenge: {
				columnFamilyName: schema.ColumnFamilyRegistrationProperties,
				columnName:       schema.ColumnChallenge,
				data:             []byte(challenge),
			},
		},
	}

	// deviceAlreadyRegisteredX expects that a DID-AID association has already
	// been completed for the DID in question
	registeredEntry = testEntry{
		key: fmt.Sprintf("%s#%s", qidRegistered, didRegistered),
		qid: qidRegistered,
		did: didRegistered,
		properties: map[string]bigtableDataEntry{
			aid: {
				columnFamilyName: schema.ColumnFamilyDeviceProperties,
				columnName:       schema.ColumnAID,
				data:             []byte(aidRegistered),
			},
			appk: {
				columnFamilyName: schema.ColumnFamilyDeviceProperties,
				columnName:       schema.ColumnAppK,
				data:             []byte(appkRegistered),
			},
			trusted: {
				columnFamilyName: schema.ColumnFamilyDeviceProperties,
				columnName:       schema.ColumnTrusted,
				data:             []byte(hardware),
			},
			challenge: {
				columnFamilyName: schema.ColumnFamilyRegistrationProperties,
				columnName:       schema.ColumnChallenge,
				data:             []byte(challenge),
			},
			registered: {
				columnFamilyName: schema.ColumnFamilyRegistrationProperties,
				columnName:       schema.ColumnRegistered,
				data:             []byte(strconv.FormatInt(time.Now().UTC().UnixMilli(), 10)),
			},
		},
	}
)

func addTestData(ctx context.Context) {
	client, err := bigtable.NewClient(ctx, schema.Project, schema.Instance)
	if err != nil {
		log.Fatalf("Could not create data operations client: %v", err)
	}
	defer client.Close()

	tbl := client.Open(schema.TableName)

	testEntires := []testEntry{theRealMcCoy, readyEntry, inFlightEntry, registeredEntry}

	for _, entry := range testEntires {
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		timestamp := bigtable.Now()
		for _, v := range entry.properties {
			mut.Set(v.columnFamilyName, v.columnName, timestamp, v.data)
		}
		tbl.Apply(ctx, entry.key, mut)
	}
}
