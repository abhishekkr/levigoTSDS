package abklevigoTSDS

import (
  "fmt"
  "time"

  levigo "github.com/jmhodges/levigo"

  ldbNS "github.com/abhishekkr/levigoNS"
)

type Time time.Time

func TimeKeyPart(key_time Time) string {
  return fmt.Sprintf("%v:%v:%v:%v:%v:%v", key_time.Year(), key_time.Month(),
                                          key_time.Day(), key_time.Hour(),
                                          key_time.Minute(), key_time.Second())
}

func KeyNameSpaceWithTime(key string, key_time Time) string{
  return fmt.Sprintf("%s:%s", key, TimeKeyPart(key_time))
}

func TimeNameSpaceWithKey(key string, key_time Time) string{
  return fmt.Sprintf("%s:%s", TimeKeyPart(key_time), key)
}

func KeyAndTimeBothNameSpace(key string, key_time Time) (string, string){
  time_ns := TimeKeyPart(key_time)
  return fmt.Sprintf("%s:%s", key, time_ns), fmt.Sprintf("%s:%s", time_ns, key)
}

func ReadTSDS(key string, db *levigo.DB){
  val := ldbNS.ReadNSRecursive(key)
  fmt.Println(val)
}

func PushTSDS(key string, val string, key_time Time, db *levigo.DB){
  keytsds := KeyNameSpaceWithTime(key, key_time)
  ldbNS.PushNS(keytsds, val, db)
}
