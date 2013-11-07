package main

import (
  "fmt"
  "time"
  "flag"

  "github.com/jmhodges/levigo"
  abkleveldb "github.com/abhishekkr/levigoNS/leveldb"

  ltsds "../../levigoTSDS/temp"
)

var (
    dbpath = flag.String("db", "/tmp/LevigoNS01", "the path to your db")
)

func main(){
  var db *levigo.DB
  fmt.Println("Your DB is referenced at", *dbpath)
  db = abkleveldb.CreateDB(*dbpath)

  ltsds.PushTSDS("127.0.0.1:ping", "up", time.Now(), db)
  ltsds.ReadTSDS("127.0.0.1:ping", db)
}
