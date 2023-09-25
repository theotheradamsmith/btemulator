package build

import (
	"context"
	"log"

	"cloud.google.com/go/bigtable"
	"github.com/theotheradamsmith/btemulator/internal/schema"
	"github.com/theotheradamsmith/btemulator/internal/util"
)

func DoAdmin(ctx context.Context, project, instance string) *bigtable.AdminClient {
	adminClient, err := bigtable.NewAdminClient(ctx, project, instance)
	if err != nil {
		log.Fatalf("Could not create admin client: %v", err)
	}

	tables, err := adminClient.Tables(ctx)
	if err != nil {
		log.Fatalf("Could not fetch table list: %v", err)
	}
	if !util.SliceContains(tables, schema.TableName) {
		log.Printf("Creating table %s", schema.TableName)
		if err := adminClient.CreateTable(ctx, schema.TableName); err != nil {
			log.Fatalf("Could not create table %s: %v", schema.TableName, err)
		}
	}

	// Make column families
	tblInfo, err := adminClient.TableInfo(ctx, schema.TableName)
	if err != nil {
		log.Fatalf("Could not read info for table %s: %v", schema.TableName, err)
	}

	for _, family := range schema.ColumnFamilies {
		if !util.SliceContains(tblInfo.Families, family) {
			if err := adminClient.CreateColumnFamily(ctx, schema.TableName, family); err != nil {
				log.Fatalf("Could not create column family %s: %v", family, err)
			}
		}
	}

	return adminClient
}
