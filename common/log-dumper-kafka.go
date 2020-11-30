package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gogf/gf/container/gmap"
	"github.com/gogf/gf/os/gcache"
	"github.com/gogf/gf/os/glog"
	"github.com/gogf/gf/text/gregex"
	"github.com/gogf/gf/util/gconv"

	"github.com/yyf330/log-operator/common/kafka"
)

var (
	TopicMap = gmap.NewStrAnyMap()
	PkgCache = gcache.New()
	LogPath  = "/tmp/kafka/"
)

type Client struct {
	Path   string
	Addr   string
	client *kafka.Client
}

// 创建kafka客户端
func NewKafkaClient(addr, group, path, topic string) *Client {
	if addr == "" {
		panic("Incomplete Kafka setting")
	}
	kafkaConfig := kafka.NewConfig()
	kafkaConfig.Servers = addr
	kafkaConfig.AutoMarkOffset = false
	kafkaConfig.GroupId = group
	kafkaConfig.Topics = topic

	return &Client{Path: path, Addr: addr, client: kafka.NewClient(kafkaConfig)}
}
func (c *Client) Run() {
	for {
		if topics, err := c.client.Topics(); err == nil {
			for _, topic := range topics {
				if !TopicMap.Contains(topic) {
					//glog.Debugfln("add new topic handle: %s", topic)
					TopicMap.Set(topic, gmap.NewStrIntMap())
					go HandlerKafkaTopic(c.Path, c.Addr, "group_log_dumper", topic)
				} else {
					//glog.Debug("no match topic:", topic)
				}
			}
		} else {
			glog.Error("###", err)
			break
		}
		time.Sleep(5 * time.Second)
	}
}

// 异步处理topic日志内容
func HandlerKafkaTopic(path, addr, group, topic string) {
	kafkaClient := NewKafkaClient(addr, group, path, topic)
	defer func() {
		kafkaClient.client.Close()
		TopicMap.Remove(topic)
	}()
	// 初始化topic offset
	offsetMap := TopicMap.Get(topic).(*gmap.StrIntMap)
	InitOffsetMap(path, topic, offsetMap)
	// 标记kafka指定topic partition的offset
	offsetMap.RLockFunc(func(m map[string]int) {
		for k, v := range m {
			if v > 0 {
				if match, _ := gregex.MatchString(`(.+)\.(\d+)`, k); len(match) == 3 {
					// 从下一条读取，这里的offset+1
					glog.Debugf("mark kafka offset - topic: %s, partition: %s, offset: %d", topic, match[2], v+1)
					kafkaClient.client.MarkOffset(topic, gconv.Int(match[2]), v+1)
				}
			}
		}
	})
	handlerChan := make(chan struct{}, 5)
	for {
		if msg, err := kafkaClient.client.Receive(); err == nil {
			// 记录offset
			key := fmt.Sprintf("%s.%d", topic, msg.Partition)
			if msg.Offset <= offsetMap.Get(key) {
				msg.MarkOffset()
				continue
			}
			handlerChan <- struct{}{}
			go func() {
				HandlerKafkaMessage(msg)
				<-handlerChan
			}()
		} else {
			glog.Error(err)
			// 如果发生错误，那么退出，
			// 下一次会重新建立连接
			break
		}
	}
}

// 处理kafka消息(使用自定义的数据结构)
func HandlerKafkaMessage(kafkaMsg *kafka.Message) (err error) {
	defer func() {
		if err == nil {
			kafkaMsg.MarkOffset()
		}
	}()
	glog.Debug("--", string(kafkaMsg.Value))
	// 重新组织path
	pkg := &Package{}
	if err = json.Unmarshal(kafkaMsg.Value, pkg); err == nil {
		msgBuffer := bytes.NewBuffer([]byte(pkg.Msg))
		msgBuffer.Write([]byte(pkg.Msg))

		glog.Println(pkg.Agent.EphemeralId, ":", msgBuffer.String())
		key := fmt.Sprintf("%s", pkg.Agent.EphemeralId)
		PkgCache.Set(key, pkg.Msg, 60000)

		AddToBufferArray(pkg, kafkaMsg)
		// 使用完毕后清理分包缓存，防止内存占用
		//for i := 1; i < pkg.Total; i++ {
		//	PkgCache.Remove(key)
		//}
	} else {
		glog.Error("eee=", err)
	}
	return
}
