package common

import (
	"bytes"
	"time"

	"github.com/gogf/gf/container/garray"
	"github.com/gogf/gf/container/gmap"
	"github.com/gogf/gf/os/gfile"
	"github.com/gogf/gf/os/glog"
	"github.com/gogf/gf/os/gmlock"
	"github.com/gogf/gf/os/gtime"
)

// 异步批量保存日志
func HandlerSavingContent() {
	// 批量写日志
	keys := BufferMap.Keys()
	for _, key := range keys {
		go func(path string) {
			// 同时只允许一个该文件的写入任务存在
			if gmlock.TryLock(path) {
				defer gmlock.Unlock(path)
			} else {
				glog.Info(path, "is already saving...")
				return
			}
			array := BufferMap.Get(path).(*garray.SortedArray)
			if array.Len() > 0 {
				minTime := int64(0)
				maxTime := int64(0)
				if array.Len() > 0 {
					// 队列最小时间与当前时间的间隔比较(而不是队列两端大小的比较)
					v, ok := array.Get(0)
					if ok {
						minTime = v.(*Item).Mtime
						maxTime = gtime.Millisecond()
					}

				}
				bu := bytes.NewBuffer(nil)
				bufferCount := 0
				tmpOffsetMap := gmap.NewStrIntMap()
				for i := 0; i < array.Len(); i++ {
					item, ok := array.Get(0)
					if !ok {
						continue
					}
					// 超过缓冲区时间则写入文件
					if maxTime-minTime >= BufferTime*1000 && bufferCount < Length {
						// 记录写入的kafka offset
						key := BuildOffsetKey(item.(*Item).Topic, item.(*Item).Partition)
						if item.(*Item).Offset > tmpOffsetMap.Get(key) {
							tmpOffsetMap.Set(key, item.(*Item).Offset)
						}
						bu.WriteString(item.(*Item).Content)
						array.PopLeft()
						bufferCount++
					} else {
						break
					}
				}
				for bu.Len() > 0 {
					if err := gfile.PutBytes(path, bu.Bytes()); err != nil {
						// 如果日志写失败，等待1秒后继续
						glog.Error(err)
						time.Sleep(time.Second)
					} else {
						// 真实写入成功之后才写入kafka offset到全局的offset哈希表中，以便磁盘化
						if tmpOffsetMap.Size() > 0 {
							for key, off := range tmpOffsetMap.Map() {
								topic, partition := ParseOffsetKey(key)
								if topic != "" {
									SetOffsetMap(topic, partition, off)
								} else {
									glog.Error("cannot parse key:", key)
								}
							}
						}
						glog.Debugf("%s : %d, %d, %d, %s, %s", path, bu.Len(), bufferCount, array.Len(),
							gtime.NewFromTimeStamp(minTime).Format("Y-m-d H:i:s.u"),
							gtime.NewFromTimeStamp(maxTime).Format("Y-m-d H:i:s.u"),
						)
						bu.Reset()
						break
					}
				}
			} else {
				//glog.Debugfln("%s empty array", path)
			}
		}(key.(string))
	}
}

// 导出topic offset到磁盘保存
func HandlerDumpOffsetMapCron(logPath, topic string) {
	TopicMap.RLockFunc(func(m map[string]interface{}) {
		for _, v := range m {
			go DumpOffsetMap(v.(*gmap.StrIntMap), logPath, topic)
		}
	})
}
