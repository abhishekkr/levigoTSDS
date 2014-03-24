package abklevigoTSDS

import (
  "testing"
  "time"

  levigo "github.com/jmhodges/levigo"

  abkleveldb "github.com/abhishekkr/levigoNS/leveldb"
  abklevigoNS "github.com/abhishekkr/levigoNS"
  golhashmap "github.com/abhishekkr/gol/golhashmap"
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

  expected_val := "upstate:2014:January:2:12:1:20,down\nupstate:2014:January:2:12:11:20,up\n"
  result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("upstate:2014:January", db))
  if (expected_val != result_val) {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  expected_val = "upstate:2014:January:2:12:11:20,up\n"
  result_val = golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("upstate:2014:January:2:12:11", db))
  if (expected_val != result_val) {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  expected_val = ""
  result_val = golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("upstate:2014:February:2:12:11", db))
  if (expected_val != result_val) {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestPushTSDS(t *testing.T) {
  anytime := time.Date(2014, 1, 2, 12, 10, 1, 0, time.UTC)
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  keytsds := PushTSDS("upstate", "up", anytime, db)
  expected_val := "upstate:2014:January:2:12:10:1,up\n"
  result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("upstate:2014:January:2:12:10:1", db))
  if (expected_val != result_val) {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if keytsds != "upstate:2014:January:2:12:10:1" {
    t.Error("PushTSDS created wrong key", keytsds, "for upstate:2014:January:2:12:10:1")
  }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestPushTSDS_BaseKey(t *testing.T) {
  anytime := time.Date(2014, 1, 2, 12, 10, 1, 0, time.UTC)
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  keytsds := PushTSDS("upstate", "up", anytime, db)
  expected_val := "upstate:2014:January:2:12:10:1,up\n"
  result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("upstate:2014:January:2:12:10:1", db))
  if (expected_val != result_val) {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if keytsds != "upstate:2014:January:2:12:10:1" {
    t.Error("PushTSDS created wrong key", keytsds, "for upstate:2014:January:2:12:10:1")
  }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestPushTSDS_BaseTime(t *testing.T) {
  anytime := time.Date(2014, 1, 2, 12, 10, 1, 0, time.UTC)
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  timetsds := PushTSDS_BaseTime("upstate", "up", anytime, db)
  expected_val := "2014:January:2:12:10:1:upstate,up\n"
  result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("2014:January:2:12:10:1:upstate", db))
  if (expected_val != result_val) {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if timetsds != "2014:January:2:12:10:1:upstate" {
    t.Error("PushTSDS created wrong key", timetsds, "for upstate:2014:January:2:12:10:1")
  }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}



func TestPushTSDS_BaseBoth(t *testing.T) {
  anytime := time.Date(2014, 1, 2, 12, 10, 1, 0, time.UTC)
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  keytsds, timetsds := PushTSDS_BaseBoth("upstate", "up", anytime, db)
  if (keytsds != "upstate:2014:January:2:12:10:1") && (timetsds != "2014:January:2:12:10:1:upstate") {
    t.Error("PushTSDS created wrong key", keytsds, "for upstate:2014:January:2:12:10:1")
  }

  expected_val := "2014:January:2:12:10:1:upstate,up\n"
  result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("2014:January:2:12:10:1:upstate", db))
  if (expected_val != result_val) {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  expected_val = "upstate:2014:January:2:12:10:1,up\n"
  result_val = golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("upstate:2014:January:2:12:10:1", db))
  if (expected_val != result_val) {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestPushNowTSDS(t *testing.T) {
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  PushNowTSDS("upstate", "up", db)

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestPushNowTSDS_BaseKey(t *testing.T) {
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  PushNowTSDS_BaseKey("upstate", "up", db)

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestPushNowTSDS_BaseTime(t *testing.T) {
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  PushNowTSDS_BaseTime("upstate", "up", db)

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestPushNowTSDS_BaseBoth(t *testing.T) {
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  PushNowTSDS_BaseBoth("upstate", "up", db)

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestDeleteTSDS(t *testing.T) {
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  DeleteTSDS("upstate:2014:January:2:12", db)
  expected_val := ""
  result_val := golhashmap.Hashmap_to_csv(abklevigoNS.ReadNSRecursive("upstate:2014:January:2:12", db))
  if (expected_val != result_val) {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}

