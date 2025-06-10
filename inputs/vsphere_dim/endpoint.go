package vsphere_dim

import (
	"context"
	"flashcat.cloud/categraf/types"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Endpoint 代表 vSphere 连接端点
type Endpoint struct {
	URL    *url.URL
	Client *Client
}

// 创建一个新的 Endpoint
func NewEndpoint(ctx context.Context, vCenterURL, username, password string, timeout time.Duration, insecureSkipVerify bool) (*Endpoint, error) {
	u, err := url.Parse(vCenterURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse vCenter URL: %v", err)
	}

	if !strings.HasSuffix(u.Path, "/sdk") {
		u.Path = strings.TrimSuffix(u.Path, "/") + "/sdk"
	}

	client, err := NewClient(ctx, u, username, password, timeout, insecureSkipVerify)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %v", err)
	}

	return &Endpoint{URL: u, Client: client}, nil
}

// 关闭 Endpoint
func (ep *Endpoint) Close(ctx context.Context) error {
	if ep.Client != nil {
		return ep.Client.Close(ctx)
	}
	return nil
}

// 查询对象
func (ep *Endpoint) QueryObjects(ctx context.Context, objectType string, properties []string, result interface{}) error {
	return ep.Client.Root.Retrieve(ctx, []string{objectType}, properties, result)
}

// 修改后的Collect方法
func (ep *Endpoint) Collect(ctx context.Context, slist *types.SampleList, ins *Instance) error {
	startTime := time.Now()
	var wg sync.WaitGroup
	errChan := make(chan error, 6) // 注意这里改为6，因为我们有6个采集任务

	// 初始化结果集
	results := &CollectionResults{
		VMs:          make(map[string]*VM),
		Datacenters:  make(map[string]*Datacenter),
		Clusters:     make(map[string]*Cluster),
		Hosts:        make(map[string]*Host),
		Datastores:   make(map[string]*Datastore),
		VirtualDisks: make([]*VirtualDisk, 0),
	}

	// 启动协程并行收集数据
	wg.Add(6)

	// 收集VM数据
	go func() {
		defer wg.Done()
		vms, err := ep.collectVM(ctx, slist, ins)
		if err != nil {
			errChan <- fmt.Errorf("failed to collect vm: %v", err)
			return
		}
		results.VMs = vms
	}()

	// 收集Host数据
	go func() {
		defer wg.Done()
		hosts, err := ep.collectHost(ctx, slist, ins)
		if err != nil {
			errChan <- fmt.Errorf("failed to collect host: %v", err)
			return
		}
		results.Hosts = hosts
	}()

	// 收集Cluster数据
	go func() {
		defer wg.Done()
		clusters, err := ep.collectCluster(ctx, slist, ins)
		if err != nil {
			errChan <- fmt.Errorf("failed to collect cluster: %v", err)
			return
		}
		results.Clusters = clusters
	}()

	// 收集Datacenter数据
	go func() {
		defer wg.Done()
		dcs, err := ep.collectDatacenter(ctx, slist, ins)
		if err != nil {
			errChan <- fmt.Errorf("failed to collect datacenter: %v", err)
			return
		}
		results.Datacenters = dcs
	}()

	// 收集Datastore数据
	go func() {
		defer wg.Done()
		ds, err := ep.collectDatastore(ctx, slist, ins)
		if err != nil {
			errChan <- fmt.Errorf("failed to collect datastore: %v", err) // 修正错误信息
			return
		}
		results.Datastores = ds
	}()

	// 收集VirtualDisk数据
	go func() {
		defer wg.Done()
		vd, err := ep.collectVirtualDisk(ctx, slist, ins)
		if err != nil {
			errChan <- fmt.Errorf("failed to collect virtual disk: %v", err) // 修正错误信息
			return
		}
		results.VirtualDisks = vd
	}()

	// 等待所有协程完成
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// 检查是否有错误
	for err := range errChan {
		return err
	}

	// 返回数据
	err := ep.processResults(results, slist, ins)
	if err != nil {
		return fmt.Errorf("failed to process results: %v", err)
	}

	elapsedTime := time.Since(startTime).Seconds()
	slist.PushSample("vsphere_dim", "exec_duration_time", elapsedTime, ins.Labels)
	return nil
}

// 处理采集结果并返回需要的数据
func (ep *Endpoint) processResults(results *CollectionResults, slist *types.SampleList, ins *Instance) error {
	// 1. 首先检查必要数据是否存在
	if len(results.VMs) == 0 {
		return fmt.Errorf("no VM data collected")
	}

	for _, vm := range results.VMs {
		labels := map[string]string{
			"apiClock":   strconv.Itoa(int(ins.CurrentTimestamp)),
			"powerState": vm.PowerState,
			"vdi":        vm.Name,
			"itemName":   inputName + "_vm_info",
			"item_name":  inputName + "_vm_info",
		}
		// 关联Host信息
		if host, exists := results.Hosts[vm.HostMoid]; exists {
			labels["vcHostName"] = host.Name
			// 关联Cluster信息
			if cluster, exists := results.Clusters[host.ClusterMoid]; exists {
				labels["clusterName"] = cluster.Name
				// 关联Datacenter信息
				if dc, exists := results.Datacenters[cluster.DatacenterMoid]; exists {
					labels["datacenter"] = dc.Name
					// 添加全局 labels
					for k, v := range ins.Labels {
						labels[k] = v
					}
					//发送数据到夜莺
					//slist.PushSample(inputName, "vm_info", 1, labels)

					// vm电源指标写入时序数据库
					powerLabels := map[string]string{
						"vdi":         vm.Name,
						"vcHostName":  host.Name,
						"clusterName": cluster.Name,
						"datacenter":  dc.Name,
						"itemName":    inputName + "_vm_power_state",
					}
					powerStateValue := 0 // 默认值
					if vm.PowerState == "poweredOn" {
						powerStateValue = 1
					}
					slist.PushSample(inputName, "vm_power_state", powerStateValue, powerLabels)

					// 发送数据到kafka
					ins.KafkaProducer.SendToKafka(labels)
				}
			}
		}
	}
	return nil
}
