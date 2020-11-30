package common

import (
	"fmt"
	"github.com/gogf/gf/container/garray"
	"github.com/gogf/gf/container/gmap"
	"github.com/gogf/gf/os/gtime"
	"github.com/gogf/gf/text/gstr"
	"github.com/gogf/gf/util/gconv"
	"github.com/yyf330/log-operator/common/kafka"
	"time"
)

var (
	BufferMap  = gmap.NewAnyAnyMap()
	Length     = 100000
	BufferTime = int64(60)
)

// 排序元素项
type Item struct {
	Mtime     int64  // 毫秒时间戳
	Content   string // 日志内容
	Offset    int    // kafka offset
	Topic     string // kafka topic
	Partition int    // kafka partition
}

// 添加日志内容到缓冲区
func AddToBufferArray(pkg *Package, kafkaMsg *kafka.Message) {
	Path := LogPath + pkg.Log.File.Path

	// array是并发安全的
	array := BufferMap.GetOrSetFuncLock(Path, func() interface{} {
		return garray.NewSortedArray(func(v1, v2 interface{}) int {
			item1 := v1.(*Item)
			item2 := v2.(*Item)
			// 两个日志项只能排前面或者后面，不能存在相等情况，否则会覆盖
			if item1.Mtime < item2.Mtime {
				return -1
			} else {
				return 1
			}
		})
	}).(*garray.SortedArray)

	// 判断缓冲区阈值
	for array.Len() > Length {
		fmt.Printf(`%s exceeds max buffer length: %d > %d, waiting..\n`, Path, array.Len(), Length)
		//log.Debugfln(`%s exceeds max buffer length: %d > %d, waiting..`, msg.Path, array.Len(), bufferLength)
		time.Sleep(time.Second)
	}

	//for _, v := range msg.Msgs {
	t := getTimeFromContent(pkg.TimeStamp)
	if t == nil || t.IsZero() {
		//glog.Debugfln(`cannot parse time from [%s] %s: %s`, msg.Host, msg.Path, v)
		t = gtime.Now()
	}
	array.Add(&Item{
		Mtime:     t.Unix(),
		Content:   pkg.Msg,
		Topic:     kafkaMsg.Topic,
		Offset:    kafkaMsg.Offset,
		Partition: kafkaMsg.Partition,
	})
	//glog.Debug("addToBufferArray:", msg.Path, k, len(msg.Msgs))
	//}
}

// 从内容中解析出日志的时间，并返回对应的日期对象
func getTimeFromContent(content string) *gtime.Time {
	if t := gtime.ParseTimeFromContent(content); t != nil {
		return t
	}
	// 为兼容以时间戳开头的傻逼格式
	// 1540973981 -- s_has_sess -- 50844917 -decryptSess- 50844917__85oxxx
	if len(content) > 10 && gstr.IsNumeric(content[0:10]) {
		return gtime.NewFromTimeStamp(gconv.Int64(content[0:10]))
	}
	return nil
}
