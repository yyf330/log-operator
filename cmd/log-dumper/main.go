// 日志消费转储端.
// 1. 定时从kafka获取完整应用日志topic列表；
// 2. 每一个topic创建不同的协程异步消费端处理；
// 3. 将kafka对应topic中的消息转储到固定的日志文件中(NAS磁盘)；
// 4. 在转储过程中需要对从各分布式节点搜集到的日志内容按照时间进行升序排序:
//     - 时间精确到毫秒
//     - 时间统一转换为UTC时区比较
//     - 缓冲区日志保留60秒的日志内容
//     - 对超过60秒的日志进行转储

package main

import (
	"fmt"
	"github.com/gogf/gf/os/gcron"
	"github.com/gogf/gf/os/glog"
	"github.com/yyf330/log-operator/common"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	LOG_PATH                  = "./log-out/"              // 日志目录
	TOPIC_AUTO_CHECK_INTERVAL = 5                         // (秒)kafka topic检测时间间隔
	HANDLER_NUM_PER_TOPIC     = "5"                       // 同一个topic消费处理时，允许并发的goroutine数量
	AUTO_SAVE_INTERVAL        = "5"                       // (秒)日志内容批量保存间隔
	KAFKA_OFFSETS_DIR_NAME    = "__dumper_offsets"        // 用于保存应用端offsets的目录名称
	KAFKA_GROUP_NAME          = "group_log_dumper"        // kafka消费端分组名称
	KAFKA_GROUP_NAME_DRYRUN   = "group_log_dumper_dryrun" // kafka消费端分组名称(dryrun)
	MAX_BUFFER_TIME_PERFILE   = "60"                      // (秒)缓冲区缓存日志的长度(按照时间衡量)
	MAX_BUFFER_LENGTH_PERFILE = "100000"                  // 缓存区日志的容量限制，当达到容量时阻塞等待日志写入后再往缓冲区添加日志
	DRYRUN                    = "false"                   // 测试运行，不真实写入文件
	DEBUG                     = "true"                    // 默认值，是否打开调试信息

)

var (
	logPath   = "./log-out/"
	kafkaAddr = "172.20.242.4:9092"
)

func main() {
	// 是否显示调试信息
	glog.SetDebug(true)

	// 定时批量写日志到文件
	gcron.Add(fmt.Sprintf(`*/%d * * * * *`, 5), common.HandlerSavingContent)


	go func() {
		kafkaClient := common.NewKafkaClient(kafkaAddr, "ttttt", logPath, "whale")
		for {
			kafkaClient.Run()
		}
	}()
	go Dump(logPath, "whale")
	go Save(logPath, "whale")
	sigch := make(chan os.Signal, 1)

	signal.Notify(sigch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// listen signal
	for sig := range sigch {
		glog.Debugf("sig recv: %s", sig.String())

		switch sig {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			close(sigch)

		case syscall.SIGHUP:

		default:
			continue
		}
	}

}
// 定时导出已处理的offset map
func Dump(logPath, topic string) {
	for {
		select {
		case <-time.After(5 * time.Second):
			common.HandlerDumpOffsetMapCron(logPath, topic)
		}
	}
}
// 定时批量写日志到文件
func Save(logPath, topic string) {
	for {
		select {
		case <-time.After(5 * time.Second):
			common.HandlerSavingContent()
		}
	}
}
