// 根据指定过期时间自动清除归档日志目录中的数据，过期时间单位为天。

package main

import (
    "github.com/gogf/gf/os/genv"
    "github.com/gogf/gf/os/gfile"
    "github.com/gogf/gf/os/glog"
    "github.com/gogf/gf/os/gtime"
    "github.com/gogf/gf/util/gconv"
    "time"
)

const (
    LOG_PATH            = "/var/log/medlinker"  // 日志目录
    EXPIRE              = "100"                 // (天)默认值，文件过期时间(超过该时间则删除文件)
    DEBUG               = "true"                // 默认值，是否打开调试信息
    AUTO_CHECK_INTERVAL = 3600                  // (秒)自动检测时间间隔
)

var (
    logPath   = genv.Get("LOG_PATH", LOG_PATH)
    debug     = gconv.Bool(genv.Get("DEBUG", DEBUG))
    expire    = gconv.Int(genv.Get("EXPIRE", EXPIRE))
)

func main() {
    glog.SetDebug(debug)

    for {
        cleanExpiredBackupFiles()
        time.Sleep(AUTO_CHECK_INTERVAL*time.Second)
    }
}

// 清除过期的备份日志文件
func cleanExpiredBackupFiles() {
    if list, err := gfile.ScanDir(logPath, "*.bz2", true); err == nil {
        for _, path := range list {
            if gfile.IsFile(path) && gtime.Second() - gfile.MTime(path) >= int64(expire * 86400) {
                if err := gfile.Remove(path); err != nil {
                    glog.Error(err)
                } else {
                    glog.Debug("removed file:", path)
                }
            }
        }
    } else {
        glog.Error(err)
    }
}
