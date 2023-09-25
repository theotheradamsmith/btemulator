package access

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/theotheradamsmith/btemulator/internal/schema"
)

var (
	ErrNoAppK         = errors.New("no Appk stored")
	ErrUnexpectedAppK = errors.New("found AppK when none should exist")
	ErrReadError      = errors.New("could not read row")
	ErrBadKey         = errors.New("invalid key format")
)

func GetAppK(ctx context.Context, tbl *bigtable.Table, key string) ([]byte, error) {
	var r bigtable.Row
	filter := bigtable.ChainFilters(bigtable.FamilyFilter(schema.ColumnFamilyDeviceProperties), bigtable.ColumnFilter(schema.ColumnAppK))
	r, err := tbl.ReadRow(ctx, key, bigtable.RowFilter(filter))
	/*
		err := tbl.ReadRows(ctx, key, func(rr bigtable.Row) bool {
			r = rr
			//item := r[schema.ColumnFamilyDeviceProperties][0]
			//log.Printf("INTERNAL: \t%s = %s", r.Key(), item.Value)
			return true
		}, bigtable.RowFilter(filter))
	*/
	if r == nil || reflect.DeepEqual(r[schema.ColumnFamilyDeviceProperties][0].Value, []byte("")) {
		return nil, fmt.Errorf("%w: key %s", ErrNoAppK, key)
	} else if err != nil {
		return nil, fmt.Errorf("%w: key %s: %v", ErrReadError, key, err)
	}
	//log.Printf("HOOHA %v", r[schema.ColumnFamilyDeviceProperties])
	//log.Printf("HOOHA %s", r[schema.ColumnFamilyDeviceProperties][0].Value)
	return r[schema.ColumnFamilyDeviceProperties][0].Value, nil
}

func AidIsPairedAndUnregistered(ctx context.Context, tbl *bigtable.Table, aid string) (bool, string, error) {
	/*
		log.Printf("\n\nNEW TEST for %s\n\n", aid)
		var r bigtable.Row
		filter := bigtable.ChainFilters(bigtable.FamilyFilter(schema.ColumnFamilyDeviceProperties), bigtable.ColumnFilter(schema.ColumnAppK))
		err := tbl.ReadRows(ctx, bigtable.PrefixRange(aid), func(rr bigtable.Row) bool {
			r = rr
			item := r[schema.ColumnFamilyDeviceProperties][0]
			log.Printf("INTERNAL: \t%s = %s", r.Key(), item.Value)
			return true
		}, bigtable.RowFilter(filter))
		if r == nil {
			return errors.New("no aid-did pairing in registration pool")
		} else if err != nil {
			return fmt.Errorf("could not read row with key %s: %v", aid, err)
		}
		if len(r[schema.ColumnFamilyDeviceProperties][0].Column) != 0 {
			//log.Printf("HOOHA %v", r[schema.ColumnFamilyDeviceProperties])
			//log.Printf("HOOHA %s", r[schema.ColumnFamilyDeviceProperties][0].Value)
			return fmt.Errorf("expected empty appk")
		}
		return err
	*/
	key, err := ReadAidRow(ctx, tbl, aid)
	if err != nil {
		return false, key, err
	}

	appk, err := GetAppK(ctx, tbl, key)
	if errors.Is(err, ErrNoAppK) {
		return true, key, nil
	}

	if appk != nil {
		err = ErrUnexpectedAppK
	}

	return false, key, err

}

func ReadAidRow(ctx context.Context, tbl *bigtable.Table, aid string) (string, error) {
	//log.Printf("readAidRow: %s\n", aid)
	//aidRow, err := tbl.ReadRow(ctx, aid)
	var r bigtable.Row
	err := tbl.ReadRows(ctx, bigtable.PrefixRange(aid), func(row bigtable.Row) bool {
		r = row
		return true
	})
	if r == nil {
		return "", errors.New("no aid-did pairing in registration pool")
	} else if err != nil {
		return "", fmt.Errorf("could not read row with key %s: %v", aid, err)
	}
	//log.Printf("\t%s = %v\n", aidRow.Key(), aidRow[schema.ColumnFamilyDeviceProperties])
	//log.Printf("\t%s = %v\n", r.Key(), r[schema.ColumnFamilyDeviceProperties])
	return r.Key(), nil
}

func ReadAllRows(ctx context.Context, tbl *bigtable.Table, columnName, columnFamilyName string) {
	log.Printf("Reading all %s in %s rows:", columnName, columnFamilyName)
	_ = tbl.ReadRows(ctx, bigtable.PrefixRange(""), func(row bigtable.Row) bool {
		item := row[columnFamilyName][0]
		log.Printf("\t%s = %s\n", item.Row, string(item.Value))
		return true
	}, bigtable.RowFilter(bigtable.ColumnFilter(columnName)))
}

func ParseRPKey(key string) (string, error) {
	sVec := strings.Split(key, "#")
	if len(sVec) != 3 {
		return "", ErrBadKey
	}
	for _, s := range sVec {
		if len(s) == 0 {
			return "", ErrBadKey
		}
	}
	return fmt.Sprintf("%s#%s", sVec[1], sVec[2]), nil
}

func GetAidRow(ctx context.Context, tbl *bigtable.Table, aid string) (bigtable.Row, error) {
	var r bigtable.Row
	err := tbl.ReadRows(ctx, bigtable.PrefixRange(aid), func(row bigtable.Row) bool {
		r = row
		return true
	})
	if r == nil {
		return nil, errors.New("no aid-did pairing in registration pool")
	} else if err != nil {
		return nil, fmt.Errorf("could not read row with key %s: %v", aid, err)
	}
	return r, nil
}
