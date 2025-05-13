package horizon

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/inputs"
	"flashcat.cloud/categraf/types"
)

const (
	inputName                = "horizon"
	loginURL                 = "/rest/login"
	centersURL               = "/rest/config/v2/virtual-centers"
	datacentersURL           = "/rest/external/v1/datacenters"
	hostsOrClustersURL       = "/rest/external/v1/hosts-or-clusters"
	datastoresURL            = "/rest/external/v1/datastores"
	desktopPoolsURL          = "/rest/entitlements/v1/desktop-pools"
	inventoryDesktopPoolsURL = "/rest/inventory/v4/desktop-pools"
	sessionsURL              = "/rest/inventory/v1/sessions"
	machinesURL              = "/rest/inventory/v1/machines"
	connectionServersURL     = "/rest/monitor/v2/connection-servers"
	adUsersOrGroupsURL       = "/rest/external/v1/ad-users-or-groups"
)

type Instance struct {
	config.InstanceConfig

	Targets         []string        `toml:"targets"`
	ResponseTimeout config.Duration `toml:"response_timeout"`
	Domain          string          `toml:"domain"`
	client          httpClient
	config.HTTPCommonConfig

	// Mappings Set the mapping of extra tags in batches
	Mappings map[string]map[string]string `toml:"mappings"`

	KafkaConfig inputs.KafkaConfig `toml:"kafka"`
	// Kafka producer
	KafkaProducer *inputs.KafkaProducer `toml:"-"`

	// 新增一个时间戳的字段，每次触发的时候，重置 秒值时间戳
	CurrentTimestamp int64

	//缓存机器列表 因为该接口查询特别慢且数据变化不是很大。所以采用缓存的方式
	machineCache struct {
		countStats  map[string]MachineCountStats // 缓存 MachineCountStats
		machineData map[string]MachineData       // 缓存 MachineData
		timestamp   time.Time
		mutex       sync.Mutex
	}
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

func (ins *Instance) Init() error {
	if len(ins.Targets) == 0 {
		return types.ErrInstancesEmpty
	}

	if ins.ResponseTimeout < config.Duration(time.Second) {
		ins.ResponseTimeout = config.Duration(time.Second * 3)
	}
	ins.InitHTTPClientConfig()
	ins.Timeout = ins.ResponseTimeout

	client, err := ins.createHTTPClient()
	if err != nil {
		return fmt.Errorf("failed to create http client: %v", err)
	}

	ins.client = client
	for _, target := range ins.Targets {
		addr, err := url.Parse(target)
		if err != nil {
			return fmt.Errorf("failed to parse http url: %s, error: %v", target, err)
		}
		if addr.Scheme != "http" && addr.Scheme != "https" {
			return fmt.Errorf("only http and https are supported, target: %s", target)
		}
	}

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

	return nil
}

func (ins *Instance) createHTTPClient() (*http.Client, error) {
	// 创建一个自定义的 HTTP Transport，禁用证书验证
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // 忽略证书验证
		},
	}
	// 创建 HTTP 客户端并使用自定义的 Transport
	client := &http.Client{
		Timeout:   time.Duration(ins.Timeout),
		Transport: transport,
	}
	return client, nil
}

type HorizonResponse struct {
	config.PluginConfig
	Instances   []*Instance                  `toml:"instances"`
	Mappings    map[string]map[string]string `toml:"mappings"`
	KafkaConfig inputs.KafkaConfig           `toml:"kafka"`
}

func init() {
	inputs.Add(inputName, func() inputs.Input {
		return &HorizonResponse{}
	})
}

func (h *HorizonResponse) Clone() inputs.Input {
	return &HorizonResponse{}
}

func (h *HorizonResponse) Name() string {
	return inputName
}

func (h *HorizonResponse) GetInstances() []inputs.Instance {
	ret := make([]inputs.Instance, len(h.Instances))
	for i := 0; i < len(h.Instances); i++ {
		if len(h.Instances[i].Mappings) == 0 {
			h.Instances[i].Mappings = h.Mappings
		} else {
			m := make(map[string]map[string]string)
			for k, v := range h.Mappings {
				m[k] = v
			}
			for k, v := range h.Instances[i].Mappings {
				m[k] = v
			}
			h.Instances[i].Mappings = m
		}
		// 将 Kafka 配置传递给每个实例
		h.Instances[i].KafkaConfig = h.KafkaConfig
		ret[i] = h.Instances[i]
	}
	return ret
}

func (ins *Instance) Gather(slist *types.SampleList) {
	if len(ins.Targets) == 0 {
		return
	}
	ins.CurrentTimestamp = time.Now().Unix()
	wg := new(sync.WaitGroup)
	for _, target := range ins.Targets {
		wg.Add(1)
		go func(target string) {
			defer wg.Done()
			ins.gather(slist, target)
		}(target)
	}
	wg.Wait()
}

func (ins *Instance) gather(slist *types.SampleList, target string) {
	if ins.DebugMod {
		log.Println("D! http_response... target:", target)
	}
	startTime := time.Now()
	labels := map[string]string{"target": target}
	// Add extra tags in batches
	if m, ok := ins.Mappings[target]; ok {
		for k, v := range m {
			labels[k] = v
		}
	}
	// 获取 token
	token, err := GetToken(ins, target, loginURL)
	if err != nil {
		log.Printf("E! Error getting token: %v", err)
		return
	}
	// 获取 datastore 信息
	ins.gatherDatastoresInfo(slist, target, token, labels)

	// 获取 desktopPool 信息
	ins.getDesktopPool(slist, target, token, labels)

	// 获取connection Servers 信息
	ins.connectionServersInfo(slist, target, token, labels)

	elapsedTime := time.Since(startTime).Seconds()
	slist.PushSample(inputName, "exec_duration_time", elapsedTime, labels)

}
