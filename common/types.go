package common

// kafka消息包
type Package struct {
	//Id    int64        `json:"id"`      // 消息包ID，当被拆包时，多个分包的包id相同
	//Seq   int          `json:"seq"`     // 序列号(当消息包被拆时用于整合打包)
	TimeStamp string       `json:"@timestamp"`
	Log       PackageLog   `json:"log"`     // 总分包数(当只有一个包时，sqp = total = 1)
	Topic     string       `json:"topic"`   //topic
	Idc       string       `json:"idc"`     //idc
	Msg       string       `json:"message"` // 消息数据
	Agent     PackageAgent `json:"agent"`   // agent
}

//{"file":{"path":"/home/logs/whale/cmdb.log.20201128"},"offset":35141888}
type PackageLog struct {
	File   PackageFile `json:"file"`
	Offset int64       `json:"offset"`
}

type PackageFile struct {
	Path string `json:"path"`
}
type PackageAgent struct {
	Id          string `json:"id"`
	Version     string `json:"version"`
	Hostname    string `json:"hostname"`
	EphemeralId string `json:"ephemeral_id"`
}

// kafka消息数据结构
type Message struct {
	Path string   `json:"path"` // 日志文件路径
	Msgs []string `json:"msgs"` // 日志内容(多条)
	Time string   `json:"time"` // 发送时间(客户端搜集时间)
	Host string   `json:"host"` // 节点主机名称
}
