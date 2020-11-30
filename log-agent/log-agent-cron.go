package main

import (
    "github.com/gogf/gf/encoding/gjson"
    "github.com/gogf/gf/os/gfile"
    "github.com/gogf/gf/os/glog"
    "github.com/gogf/gf/os/gtime"
)

// 自动清理日志文件
func cleanLogCron() {
    if list, err := gfile.ScanDir(logPath, "*.log", true); err == nil {
        for _, path := range list {
            if !gfile.IsFile(path) || gfile.Size(path) == 0 {
                continue
            }
            if gtime.Second() - gfile.MTime(path) > bufferTime {
                // 判断文件时间
                err := (error)(nil)
                glog.Debug("[log-clean] truncate expired file:", path)
                if !dryrun {
                    err = gfile.Truncate(path, 0)
                }
                if err != nil {
                    glog.Error(err)
                }
            } else {
                // 判断文件大小，超过指定大小则truncate
                if gfile.Size(path) > cleanMaxSize {
                    glog.Debug("[log-clean] truncate size-exceeded file:", path)
                    if !dryrun {
                        if err := gfile.Truncate(path, 0); err != nil {
                            glog.Error(err)
                        }
                    }
                } else {
                    glog.Debug("[log-clean] leave alone file:", path)
                }
            }
        }
    } else {
        glog.Error(err)
    }
}

// 定时保存日志文件的offset记录到文件中
func saveOffsetCron() {
    if offsetMapSave.Size() == 0 {
        return
    }
    if content, err := gjson.Encode(offsetMapSave.Clone()); err != nil {
        glog.Error(err)
    } else {
        if err := gfile.PutBinContents(offsetFilePath, content); err != nil {
            glog.Error(err)
        }
    }
}
