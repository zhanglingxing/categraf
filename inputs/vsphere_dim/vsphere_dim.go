package vsphere_dim

import (
	"context"
	"fmt"
	"log"
	"time"

	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/inputs"
	"flashcat.cloud/categraf/types"
)

const inputName = "vsphere_dim"

// VSphereDIM 插件主类
type VSphereDIM struct {
	config.PluginConfig
	Instances   []*Instance        `toml:"instances"`
	KafkaConfig inputs.KafkaConfig `toml:"kafka"`
}

func init() {
	inputs.Add(inputName, func() inputs.Input {
		return &VSphereDIM{}
	})
}

func (vs *VSphereDIM) Clone() inputs.Input {
	return &VSphereDIM{}
}

func (vs *VSphereDIM) Name() string {
	return inputName
}

func (pt *VSphereDIM) GetInstances() []inputs.Instance {
	ret := make([]inputs.Instance, len(pt.Instances))
	for i := 0; i < len(pt.Instances); i++ {
		ret[i] = pt.Instances[i]
		// 将 Kafka 配置传递给每个实例
		pt.Instances[i].KafkaConfig = pt.KafkaConfig
		ret[i] = pt.Instances[i]
	}
	return ret
}

type Instance struct {
	config.InstanceConfig

	Vcenter            string          `toml:"vcenter"`
	Username           string          `toml:"username"`
	Password           string          `toml:"password"`
	Timeout            config.Duration `toml:"timeout"`
	InsecureSkipVerify bool            `toml:"insecure_skip_verify"` // 是否跳过证书验证

	endpoints *Endpoint
	cancel    context.CancelFunc

	KafkaConfig inputs.KafkaConfig `toml:"kafka"`
	// Kafka producer
	KafkaProducer *inputs.KafkaProducer `toml:"-"`
	// 新增一个时间戳的字段，每次触发的时候，重置 秒值时间戳
	CurrentTimestamp int64
}

func (ins *Instance) Init() error {
	if ins.Vcenter == "" {
		return types.ErrInstancesEmpty
	}

	ctx, cancel := context.WithCancel(context.Background())
	ins.cancel = cancel

	// 创建 Endpoint
	ep, err := NewEndpoint(ctx, ins.Vcenter, ins.Username, ins.Password, time.Duration(ins.Timeout), ins.InsecureSkipVerify)
	if err != nil {
		return fmt.Errorf("failed to create endpoint: %v", err)
	}
	ins.endpoints = ep

	// 初始化 Kafka 生产者
	if len(ins.KafkaConfig.Brokers) > 0 {
		producer, err := inputs.NewKafkaProducer(ins.KafkaConfig)
		if err != nil {
			return fmt.Errorf("failed to create Kafka producer: %v", err)
		} else {
			log.Printf("I! Kafka producer init success: %v", ins.KafkaConfig.Brokers)
		}
		ins.KafkaProducer = producer
	}
	log.Printf("I! Successfully connected to vCenter: %s", ins.Vcenter)

	return nil
}

func (ins *Instance) Drop() {
	log.Printf("I! Stopping plugin")
	ins.cancel()
	if ins.endpoints != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ins.Timeout))
		defer cancel()
		if err := ins.endpoints.Close(ctx); err != nil {
			log.Printf("E! Failed to close endpoint: %v", err)
		}
	}
}

// Gather 采集数据
func (ins *Instance) Gather(slist *types.SampleList) {
	ctx := context.Background()
	ins.CurrentTimestamp = time.Now().Unix()
	// 调用 Collect 方法采集数据
	if err := ins.endpoints.Collect(ctx, slist, ins); err != nil {
		log.Printf("E! Failed to collect data: %v", err)
	}
}
