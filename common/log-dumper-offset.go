package common

import (
	"fmt"
	"github.com/gogf/gf/container/gmap"
	"github.com/gogf/gf/os/gfile"
	"github.com/gogf/gf/text/gregex"
	"github.com/gogf/gf/util/gconv"
)

// 生成kafka消费offset文件路径
func offsetFilePath(path, dir, offsetKey string) string {
	return fmt.Sprintf("%s/%s/%s.offset", path, dir, offsetKey)
}

// 初始化topic offset
func InitOffsetMap(path, topic string, offsetMap *gmap.StrIntMap) {
	for i := 0; i < 100; i++ {
		key := BuildOffsetKey(topic, i)
		path := offsetFilePath(path, topic, key)
		if !gfile.Exists(path) {
			break
		}
		offsetMap.Set(key, gconv.Int(gfile.GetContents(path)))
	}
}

// 根据topic和partition生成key
func BuildOffsetKey(topic string, partition int) string {
	return fmt.Sprintf("%s.%d", topic, partition)
}

// 从key中解析出topic和partition，是buildOffsetKey的相反操作
func ParseOffsetKey(key string) (topic string, partition int) {
	match, _ := gregex.MatchString(`(.+)\.(\d+)`, key)
	if len(match) > 0 {
		return match[1], gconv.Int(match[2])
	}
	return "", 0
}

// 设置topic offset
func SetOffsetMap(topic string, partition int, offset int) {
	key := BuildOffsetKey(topic, partition)
	TopicMap.RLockFunc(func(m map[string]interface{}) {
		if r, ok := m[topic]; ok {
			r.(*gmap.StrIntMap).Set(key, offset)
		}
	})
}

// 应用自定义保存当前kafka读取的offset
func DumpOffsetMap(offsetMap *gmap.StrIntMap, logPath, topic string) {
	if offsetMap.Size() == 0 {
		return
	}
	offsetMap.RLockFunc(func(m map[string]int) {
		for key, offset := range m {
			if offset == 0 {
				continue
			}
			gfile.PutContents(offsetFilePath(logPath, topic, key), gconv.String(offset))
		}
	})
}
