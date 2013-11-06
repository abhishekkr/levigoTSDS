package abklevigoTSDS

import (
  "fmt"
  "time"

  levigo "github.com/jmhodges/levigo"

  ldbNS "github.com/abhishekkr/levigoNS"
)

func TimeKeyPart(key_time time) string {
  return fmt.Sprintf("Time:%q", key_time)
}

func KeyNameSpaceWithTime(key string, key_time time) string{
  return fmt.Sprintf("%s:%s", key, TimeKeyPart(key_time))
}

func TimeNameSpaceWithKey(key string, key_time time) string{
  return fmt.Sprintf("%s:%s", TimeKeyPart(key_time), key)
}

func KeyAndTimeBothNameSpace(key string, key_time time) string{
  time_ns := TimeKeyPart(key_time)
  return fmt.Sprintf("%s:%s", key, time_ns), fmt.Sprintf("%s:%s", time_ns, key)
}

func ReadTSDS(key string, db *levigo.DB){
  val := ldbNS.ReadNSRecursive(key)
  fmt.Println(val)
}

func PushTSDS(key string, val string, key_time time, db *levigo.DB){
  keytsds := KeyNameSpaceWithTime(key, key_time)
  ldbNS.PushNS(keytsds, val, db)
}
