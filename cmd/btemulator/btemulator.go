package main

import (
	"context"
	"flag"
	"log"

	"github.com/theotheradamsmith/btemulator/internal/access"
	"github.com/theotheradamsmith/btemulator/internal/build"
	"github.com/theotheradamsmith/btemulator/internal/schema"
)

func main() {
	project := flag.String("project", schema.Project, "The Google Cloud Platform project ID. Required.")
	instance := flag.String("instance", schema.Instance, "The Google Cloud Bigtable instance ID. Required.")

	flag.Parse()

	for _, f := range []string{"project", "instance"} {
		if flag.Lookup(f).Value.String() == "" {
			log.Fatalf("The %s flag is required.", f)
		}
	}

	ctx := context.Background()

	admin := build.DoAdmin(ctx, *project, *instance)

	client, tbl := build.DoClient(ctx, *project, *instance)

	access.ReadAllRows(ctx, tbl, schema.ColumnDID, schema.ColumnFamilyDeviceProperties)
	access.ReadAllRows(ctx, tbl, schema.ColumnMainKey, schema.ColumnFamilyDeviceProperties)
	access.ReadAllRows(ctx, tbl, schema.ColumnAID, schema.ColumnFamilyDeviceProperties)

	access.ReadAidRow(ctx, tbl, "aid-1")
	access.ReadAidRow(ctx, tbl, "aid-2")
	access.ReadAidRow(ctx, tbl, "aid-3")

	if err := client.Close(); err != nil {
		log.Fatalf("Could not close data operations client: %v", err)
	}

	/*
		log.Printf("Deleting the table")
		if err := admin.DeleteTable(ctx, schema.TableName); err != nil {
			log.Fatalf("Could not delete table %s: %v", schema.TableName, err)
		}
	*/

	if err := admin.Close(); err != nil {
		log.Fatalf("Could not close admin client: %v", err)
	}
}

/*
const (
	tableName        = "Hello-Bigtable"
	columnFamilyName = "cf1"
	columnName       = "greeting"
)

var greetings = []string{"Hello World!", "Hello Cloud Bigtable!", "Hello golang!"}
*/

/*
func doAdmin(ctx context.Context, project, instance string) *bigtable.AdminClient {
	adminClient, err := bigtable.NewAdminClient(ctx, project, instance)
	if err != nil {
		log.Fatalf("Could not create admin client: %v", err)
	}

	tables, err := adminClient.Tables(ctx)
	if err != nil {
		log.Fatalf("Could not fetch table list: %v", err)
	}

	fmt.Println(tables)

	if !sliceContains(tables, tableName) {
		log.Printf("Creating table %s", tableName)
		if err := adminClient.CreateTable(ctx, tableName); err != nil {
			log.Fatalf("Could not create table %s: %v", tableName, err)
		}
	}

	tblInfo, err := adminClient.TableInfo(ctx, tableName)
	if err != nil {
		log.Fatalf("Could not read info for table %s: %v", tableName, err)
	}

	if !sliceContains(tblInfo.Families, columnFamilyName) {
		if err := adminClient.CreateColumnFamily(ctx, tableName, columnFamilyName); err != nil {
			log.Fatalf("Could not create column family %s: %v", columnFamilyName, err)
		}
	}

	return adminClient
}
*/

/*
func doClient(ctx context.Context, project, instance string) *bigtable.Client {
	client, err := bigtable.NewClient(ctx, project, instance)
	if err != nil {
		log.Fatalf("Could not create data operations client: %v", err)
	}

	tbl := client.Open(tableName)
	muts := make([]*bigtable.Mutation, len(greetings))
	rowKeys := make([]string, len(greetings))

	log.Printf("Writing greeting rows to table")
	for i, greeting := range greetings {
		muts[i] = bigtable.NewMutation()
		muts[i].Set(columnFamilyName, columnName, bigtable.Now(), []byte(greeting))

		rowKeys[i] = fmt.Sprintf("%s%d", columnName, i)
	}

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

	log.Printf("Getting a single greeting by row key:")
	row, err := tbl.ReadRow(ctx, rowKeys[0], bigtable.RowFilter(bigtable.ColumnFilter(columnName)))
	if err != nil {
		log.Fatalf("Could not read row with key %s: %v", rowKeys[0], err)
	}
	log.Printf("\t%s = %s\n", rowKeys[0], string(row[columnFamilyName][0].Value))

	log.Printf("Reading all greeting rows:")
	_ = tbl.ReadRows(ctx, bigtable.PrefixRange(columnName), func(row bigtable.Row) bool {
		item := row[columnFamilyName][0]
		log.Printf("\t%s = %s\n", item.Row, string(item.Value))
		return true
	}, bigtable.RowFilter(bigtable.ColumnFilter(columnName)))
	return client
}
*/
