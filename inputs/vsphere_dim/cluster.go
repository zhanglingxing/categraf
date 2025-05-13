package vsphere_dim

import (
	"context"
	"flashcat.cloud/categraf/types"
	"fmt"
	"github.com/vmware/govmomi/vim25/mo"
	"log"
)

// Cluster 集群信息结构体
type Cluster struct {
	// 标识信息
	Name           string // 集群名称
	Moid           string // 集群唯一ID
	DatacenterMoid string // 所属数据中心ID

	HaEnabled          bool   // HA 是否开启
	HaVmMonitoring     string //  主机监控策略（如 enabled 或 disabled）
	HaHostMonitoring   string //  虚拟机监控级别（如 vmMonitoringOnly, vmAndAppMonitoring, disabled）
	HaAdmissionControl bool   //  是否启用准入控制（防止资源不足时启动新 VM）

	// 配置信息
	DrsEnabled         bool   // DRS 是否开启
	DrsAutomationLevel string // DRS 自动化级别（manual, partiallyAutomated, fullyAutomated）
}

// collectCluster 收集集群信息
func (ep *Endpoint) collectCluster(ctx context.Context, slist *types.SampleList, ins *Instance) (map[string]*Cluster, error) {
	clusterMap := make(map[string]*Cluster)
	var clusters []mo.ClusterComputeResource

	// 查询集群信息
	err := ep.QueryObjects(ctx, "ClusterComputeResource", []string{
		"name",
		"parent",
		"configuration.drsConfig",
		"configuration.dasConfig",
	}, &clusters)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve clusters: %v", err)
	}

	for _, c := range clusters {
		cluster := &Cluster{
			Name: c.Name,
			Moid: c.Self.Value,
		}

		// 查询父对象（假设父对象是文件夹类型）
		var parentFolder []mo.Folder
		err := ep.QueryObjects(ctx, "Folder", []string{"parent"}, &parentFolder)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve parent folder for cluster %s: %v", c.Self.Value, err)
		}
		for _, folder := range parentFolder {
			if folder.Parent != nil && folder.Parent.Type == "Datacenter" {
				cluster.DatacenterMoid = folder.Parent.Value
				break
			}
		}

		// 获取 DRS 设置
		if c.Configuration.DrsConfig.Enabled != nil {
			cluster.DrsEnabled = *c.Configuration.DrsConfig.Enabled
			cluster.DrsAutomationLevel = string(c.Configuration.DrsConfig.DefaultVmBehavior)
		}

		// 获取 HA 设置
		if c.Configuration.DasConfig.Enabled != nil {
			cluster.HaEnabled = *c.Configuration.DasConfig.Enabled
			cluster.HaVmMonitoring = c.Configuration.DasConfig.VmMonitoring
			cluster.HaHostMonitoring = c.Configuration.DasConfig.HostMonitoring
			cluster.HaAdmissionControl = *c.Configuration.DasConfig.AdmissionControlEnabled
		}
		// 打印集群信息
		// printCluster(cluster)
		clusterMap[cluster.Moid] = cluster
	}
	return clusterMap, nil
}

func printCluster(cluster *Cluster) {
	// 打印集群信息
	log.Println("========= 集群信息 =========")
	log.Printf("%-30s: %s", "Name (集群名称)", cluster.Name)
	log.Printf("%-30s: %s", "Moid (集群唯一ID)", cluster.Moid)
	log.Printf("%-30s: %s", "DatacenterMoid (所属数据中心ID)", cluster.DatacenterMoid)

	log.Printf("%-30s: %t", "DrsEnabled (DRS 是否开启)", cluster.DrsEnabled)
	log.Printf("%-30s: %s", "DrsAutomationLevel (DRS 自动化级别)", cluster.DrsAutomationLevel)

	log.Printf("%-30s: %t", "HaEnabled (HA 是否开启)", cluster.HaEnabled)
	log.Printf("%-30s: %s", "HaVmMonitoring (VM 监控级别)", cluster.HaVmMonitoring)
	log.Printf("%-30s: %s", "HaHostMonitoring (主机监控)", cluster.HaHostMonitoring)
	log.Printf("%-30s: %t", "HaAdmissionControl (HA 准入控制)", cluster.HaAdmissionControl)
	log.Println("=================================")
}
