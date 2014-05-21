package abklevigoTSDS

import (
	"testing"
	"time"

	levigo "github.com/jmhodges/levigo"

	golhashmap "github.com/abhishekkr/gol/golhashmap"
	abklevigoNS "github.com/abhishekkr/levigoNS"
	abkleveldb "github.com/abhishekkr/levigoNS/leveldb"
)

var (
	dbpath = "/tmp/delete-this-levigoTSDS"
)

func setupTestData(db *levigo.DB) {
	abklevigoNS.PushNS("upstate:2014:January:2:12:1:20", "down", db)
	abklevigoNS.PushNS("2014:January:2:12:1:20:upstate", "down", db)

	abklevigoNS.PushNS("upstate:2014:January:2:12:11:20", "up", db)
	abklevigoNS.PushNS("2014:January:2:12:11:20:upstate", "up", db)
}

func TestTimeKeyPart(t *testing.T) {
	anytime := time.Date(2014, 1, 2, 12, 10, 1, 0, time.UTC)

	expected_val := "2014:January:2:12:10:1"
	result_val := TimeKeyPart(anytime)
	if expected_val != result_val {
		t.Error("Fail: Get", result_val, "instead of", expected_val)
	}
}

func TestKeyNameSpaceWithTime(t *testing.T) {
	anytime := time.Date(2014, 1, 2, 12, 10, 1, 0, time.UTC)

	expected_val := "upstate:2014:January:2:12:10:1"
	result_val := KeyNameSpaceWithTime("upstate", anytime)
	if expected_val != result_val {
		t.Error("Fail: Get", result_val, "instead of", expected_val)
	}
}

func TestTimeNameSpaceWithKey(t *testing.T) {
	anytime := time.Date(2014, 1, 2, 12, 10, 1, 0, time.UTC)

	expected_val := "2014:January:2:12:10:1:upstate"
	result_val := TimeNameSpaceWithKey("upstate", anytime)
	if expected_val != result_val {
		t.Error("Fail: Get", result_val, "instead of", expected_val)
	}
}

func TestKeyAndTimeBothNameSpace(t *testing.T) {
	anytime := time.Date(2014, 1, 2, 12, 10, 1, 0, time.UTC)

	expected_val1, expected_val2 := "upstate:2014:January:2:12:10:1", "2014:January:2:12:10:1:upstate"
	result_val1, result_val2 := KeyAndTimeBothNameSpace("upstate", anytime)
	if (expected_val1 != result_val1) || (expected_val2 != result_val2) {
		t.Error("Fail: Get", result_val1, "and", result_val2, "instead of", expected_val1, "and", expected_val2)
	}
}

func TestReadTSDS(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	expected_val := "upstate:2014:January:2:12:1:20,down\nupstate:2014:January:2:12:11:20,up"
	result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("upstate:2014:January", db))
	if expected_val != result_val {
		t.Error("Fail: Get", result_val, "instead of", expected_val)
	}

	expected_val = "upstate:2014:January:2:12:11:20,up"
	result_val = golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("upstate:2014:January:2:12:11", db))
	if expected_val != result_val {
		t.Error("Fail: Get", result_val, "instead of", expected_val)
	}

	expected_val = ""
	result_val = golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("upstate:2014:February:2:12:11", db))
	if expected_val != result_val {
		t.Error("Fail: Get", result_val, "instead of", expected_val)
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestPushTSDS(t *testing.T) {
	anytime := time.Date(2014, 1, 2, 12, 10, 1, 0, time.UTC)
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	if !PushTSDS("upstate", "up", anytime, db) {
		t.Error("PushTSDS creation failed for upstate:2014:January:2:12:10:1")
	}
	expected_val := "upstate:2014:January:2:12:10:1,up"
	result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("upstate:2014:January:2:12:10:1", db))
	if expected_val != result_val {
		t.Error("Fail: Get", result_val, "instead of", expected_val)
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestPushTSDS_BaseKey(t *testing.T) {
	anytime := time.Date(2014, 1, 2, 12, 10, 1, 0, time.UTC)
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	if !PushTSDS_BaseKey("upstate", "up", anytime, db) {
		t.Error("PushTSDS_BaseKey creation failed for upstate:2014:January:2:12:10:1")
	}

	expected_val := "upstate:2014:January:2:12:10:1,up"
	result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("upstate:2014:January:2:12:10:1", db))
	if expected_val != result_val {
		t.Error("Fail: Get", result_val, "instead of", expected_val)
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestPushTSDS_BaseTime(t *testing.T) {
	anytime := time.Date(2014, 1, 2, 12, 10, 1, 0, time.UTC)
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	if !PushTSDS_BaseTime("upstate", "up", anytime, db) {
		t.Error("PushTSDS_BaseTime creation failed for upstate:2014:January:2:12:10:1")
	}
	expected_val := "2014:January:2:12:10:1:upstate,up"
	result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("2014:January:2:12:10:1:upstate", db))
	if expected_val != result_val {
		t.Error("Fail: Get", result_val, "instead of", expected_val)
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestPushTSDS_BaseBoth(t *testing.T) {
	anytime := time.Date(2014, 1, 2, 12, 10, 1, 0, time.UTC)
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	if !PushTSDS_BaseBoth("upstate", "up", anytime, db) {
		t.Error("PushTSDS_BaseBoth creation failed for upstate:2014:January:2:12:10:1")
	}

	expected_val := "2014:January:2:12:10:1:upstate,up"
	result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("2014:January:2:12:10:1:upstate", db))
	if expected_val != result_val {
		t.Error("Fail: Get", result_val, "instead of", expected_val)
	}

	expected_val = "upstate:2014:January:2:12:10:1,up"
	result_val = golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("upstate:2014:January:2:12:10:1", db))
	if expected_val != result_val {
		t.Error("Fail: Get", result_val, "instead of", expected_val)
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestPushNowTSDS(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	if !PushNowTSDS("TestPushNowTSDS", "up", db) {
		t.Error("PushNowTSDS creation failed.")
	}

	result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("TestPushNowTSDS", db))
	if len(result_val) == 1 {
		t.Error("Fail: Wrong count of Key creation")
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestPushNowTSDS_BaseKey(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	if !PushNowTSDS_BaseKey("PushNowTSDS_BaseKey", "up", db) {
		t.Error("PushNowTSDS_BaseKey creation failed.")
	}

	result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("PushNowTSDS_BaseKey", db))
	if len(result_val) == 1 {
		t.Error("Fail: Wrong count of Key creation")
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestPushNowTSDS_BaseTime(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	if !PushNowTSDS_BaseTime("PushNowTSDS_BaseTime", "up", db) {
		t.Error("PushNowTSDS_BaseTime creation failed.")
	}

	result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("PushNowTSDS_BaseTime", db))
	if len(result_val) == 1 {
		t.Error("Fail: Wrong count of Key creation")
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestPushNowTSDS_BaseBoth(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	if !PushNowTSDS_BaseBoth("PushNowTSDS_BaseBoth", "up", db) {
		t.Error("PushNowTSDS_BaseBoth creation failed.")
	}

	result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("PushNowTSDS_BaseBoth", db))
	if len(result_val) == 1 {
		t.Error("Fail: Wrong count of Key creation")
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestDeleteTSDS(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	if !DeleteTSDS("upstate:2014:January:2:12", db) {
		t.Error("Fail: Deletion of upstate:2014:January:2:12 failed")
	}
	expected_val := ""
	result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("upstate:2014:January:2:12", db))
	if expected_val != result_val {
		t.Error("Fail: Get", result_val, "instead of", expected_val)
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}
