package abklevigoTSDS

import (
  "fmt"
  "time"

  levigo "github.com/jmhodges/levigo"

  ldbNS "github.com/abhishekkr/levigoNS"
)


func TimeKeyPart(key_time time.Time) string {
  return fmt.Sprintf("%v:%v:%v:%v:%v:%v", key_time.Year(), key_time.Month(),
                                          key_time.Day(), key_time.Hour(),
                                          key_time.Minute(), key_time.Second())
}

func KeyNameSpaceWithTime(key string, key_time time.Time) string{
  return fmt.Sprintf("%s:%s", key, TimeKeyPart(key_time))
}

func TimeNameSpaceWithKey(key string, key_time time.Time) string{
  return fmt.Sprintf("%s:%s", TimeKeyPart(key_time), key)
}

func KeyAndTimeBothNameSpace(key string, key_time time.Time) (string, string){
  time_ns := TimeKeyPart(key_time)
  return fmt.Sprintf("%s:%s", key, time_ns), fmt.Sprintf("%s:%s", time_ns, key)
}

func ReadTSDS(key string, db *levigo.DB) ldbNS.HashMap{
  return ldbNS.ReadNSRecursive(key, db)
}

func PushTSDS(key string, val string, key_time time.Time, db *levigo.DB) string{
  keytsds := KeyNameSpaceWithTime(key, key_time)
  ldbNS.PushNS(keytsds, val, db)
  return keytsds
}

func PushTSDS_BaseKey(key string, val string, key_time time.Time, db *levigo.DB) string{
  return PushTSDS(key, val, key_time, db)
}

func PushTSDS_BaseTime(key string, val string, key_time time.Time, db *levigo.DB) string{
  timetsds := TimeNameSpaceWithKey(key, key_time)
  ldbNS.PushNS(timetsds, val, db)
  return timetsds
}

func PushTSDS_BaseBoth(key string, val string, key_time time.Time, db *levigo.DB) (string, string){
  keytsds := KeyNameSpaceWithTime(key, key_time)
  ldbNS.PushNS(keytsds, val, db)

  timetsds := TimeNameSpaceWithKey(key, key_time)
  ldbNS.PushNS(timetsds, val, db)

  return keytsds, timetsds
}

func DeleteTSDS(key string, db *levigo.DB) ldbNS.HashMap{
  ldbNS.DeleteNSRecursive(key, db)
  return ldbNS.ReadNSRecursive(key, db)
}
