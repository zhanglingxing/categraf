package vsphere_dim

import (
	"context"
	"flashcat.cloud/categraf/types"
	"fmt"
	"github.com/vmware/govmomi/vim25/mo"
	"log"
)

// Host 主机信息结构体
type Host struct {
	// 标识信息
	Moid        string // 主机唯一ID
	Name        string // 主机名称
	ClusterMoid string // 所属集群ID

	// 状态信息
	ConnectionState string // 连接状态
	PowerState      string // 电源状态

	// CPU信息
	NumCpuCores    int16  // CPU核心数
	NumCpuThreads  int16  // CPU线程数
	CpuMhz         int32  // CPU总频率MHz
	CpuUsageMHz    int32  // CPU使用MHz
	CpuDescription string // CPU型号
	NumCpuSockets  int16  // CPU插槽数

	// 内存信息
	MemorySizeMB  int64 // 总内存MB
	MemoryUsageMB int32 // 使用内存MB

	// 虚拟机信息
	VmSize int32 // 虚拟机数量

	// 硬件信息
	ServerVendor string // 服务器厂商
	ServerModel  string // 服务器型号

	// 软件信息
	OperatingSystem   string // 操作系统
	HypervisorVersion string // 虚拟化版本
	HypervisorBuild   string // 虚拟化Build版本

	// 存储信息
	DatastoreIDs []string // 用切片存储数据存储ID
}

// collectHost 收集主机信息
func (ep *Endpoint) collectHost(ctx context.Context, slist *types.SampleList, ins *Instance) (map[string]*Host, error) {
	hostMap := make(map[string]*Host)
	var hosts []mo.HostSystem

	err := ep.QueryObjects(ctx, "HostSystem", []string{
		"name",
		"parent",
		"summary.runtime",
		"summary.hardware",
		"summary.quickStats",
		"summary.config.product",
		"datastore",
		"vm",
	}, &hosts)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve hosts: %v", err)
	}

	for _, h := range hosts {

		host := &Host{
			Name:        h.Name,
			Moid:        h.Self.Value,
			ClusterMoid: h.Parent.Value,
		}
		if h.Vm != nil {
			host.VmSize = int32(len(h.Vm))
		}
		// 获取主机挂载的数据存储
		if len(h.Datastore) > 0 {
			datastoreIDs := make([]string, 0, len(h.Datastore))
			for _, ds := range h.Datastore {
				datastoreIDs = append(datastoreIDs, ds.Value)
			}
			host.DatastoreIDs = datastoreIDs
		}

		if h.Summary.Runtime != nil {
			host.ConnectionState = string(h.Summary.Runtime.ConnectionState)
			host.PowerState = string(h.Summary.Runtime.PowerState)
		}

		if h.Summary.Hardware != nil {
			host.MemorySizeMB = h.Summary.Hardware.MemorySize / (1024 * 1024)
			host.NumCpuCores = h.Summary.Hardware.NumCpuCores
			host.NumCpuThreads = h.Summary.Hardware.NumCpuThreads
			cpuMhzPerCore := h.Summary.Hardware.CpuMhz
			host.NumCpuSockets = h.Summary.Hardware.NumCpuPkgs
			host.CpuMhz = cpuMhzPerCore * int32(host.NumCpuCores)
			host.CpuDescription = h.Summary.Hardware.CpuModel
			host.ServerVendor = h.Summary.Hardware.Vendor
			host.ServerModel = h.Summary.Hardware.Model
		}

		host.MemoryUsageMB = h.Summary.QuickStats.OverallMemoryUsage
		host.CpuUsageMHz = h.Summary.QuickStats.OverallCpuUsage

		if h.Summary.Config.Product != nil {
			host.HypervisorVersion = h.Summary.Config.Product.Version
			host.HypervisorBuild = h.Summary.Config.Product.Build
			host.OperatingSystem = h.Summary.Config.Product.FullName
		}
		// 打印主机信息
		// printHost(host)
		hostMap[host.Moid] = host
	}
	return hostMap, nil
}

func printHost(host *Host) {
	// 打印数据中心信息
	log.Println("========= 主机信息 =========")
	log.Printf("%-25s: %s", "Moid (主机唯一ID)", host.Moid)
	log.Printf("%-25s: %s", "Name (主机名称)", host.Name)
	log.Printf("%-25s: %s", "ClusterMoid (所属集群ID)", host.ClusterMoid)

	log.Printf("%-25s: %s", "ConnectionState (连接状态)", host.ConnectionState)
	log.Printf("%-25s: %s", "PowerState (电源状态)", host.PowerState)

	log.Printf("%-25s: %d", "NumCpuCores (CPU核心数)", host.NumCpuCores)
	log.Printf("%-25s: %d", "NumCpuThreads (CPU线程数)", host.NumCpuThreads)
	log.Printf("%-25s: %d", "CpuMhz (CPU总频率MHz)", host.CpuMhz)
	log.Printf("%-25s: %d", "CpuUsageMHz (CPU使用MHz)", host.CpuUsageMHz)
	log.Printf("%-25s: %d", "NumCpuSockets (CPU插槽数)", host.NumCpuSockets)
	log.Printf("%-25s: %s", "CpuDescription (CPU型号)", host.CpuDescription)

	log.Printf("%-25s: %d", "MemorySizeMB (总内存MB)", host.MemorySizeMB)
	log.Printf("%-25s: %d", "MemoryUsageMB (使用内存MB)", host.MemoryUsageMB)

	log.Printf("%-25s: %d", "VmSize (虚拟机数量)", host.VmSize)

	log.Printf("%-25s: %s", "ServerVendor (服务器厂商)", host.ServerVendor)
	log.Printf("%-25s: %s", "ServerModel (服务器型号)", host.ServerModel)

	log.Printf("%-25s: %s", "OperatingSystem (操作系统)", host.OperatingSystem)
	log.Printf("%-25s: %s", "HypervisorVersion (虚拟化版本)", host.HypervisorVersion)
	log.Printf("%-25s: %s", "HypervisorBuild (虚拟化Build版本)", host.HypervisorBuild)
	log.Printf("%-25s: %v", "DatastoreIDs (数据存储ID)", host.DatastoreIDs)

	log.Println("============================")
}
