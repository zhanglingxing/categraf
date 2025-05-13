package vsphere_dim

import (
	"context"
	"flashcat.cloud/categraf/types"
	"fmt"
	"github.com/vmware/govmomi/vim25/mo"
	"log"
)

// Datastore 数据存储信息结构体
type Datastore struct {
	// 标识信息
	Moid string // 数据存储唯一ID
	Name string // 数据存储名称
	Type string // 数据存储类型
	Path string // 数据存储路径

	// 容量信息
	Capacity         int64 // 总容量
	FreeSpace        int64 // 可用空间
	UsedSpace        int64 // 已使用空间
	ProvisionedSpace int64 // 已分配空间

	// 状态信息
	Accessible             bool   // 是否可访问
	MaintenanceMode        string // 维护模式
	MultipleHostAccess     bool   // 是否允许多个主机访问
	StorageIOControl       bool   // 是否启用存储 I/O 控制
	StatsCollectionEnabled bool   // 是否启用统计收集

	// 关联信息
	HostMoids []string // 关联主机的 MoID
	VmMoids   []string // 关联虚拟机的 MoID
}

// collectDatastore 收集数据存储信息
func (ep *Endpoint) collectDatastore(ctx context.Context, slist *types.SampleList, ins *Instance) (map[string]*Datastore, error) {
	datastoreMap := make(map[string]*Datastore)
	var datastores []mo.Datastore

	err := ep.QueryObjects(ctx, "Datastore", []string{
		"name",
		"summary",
		"capability",
		"iormConfiguration",
		"host",
		"vm",
	}, &datastores)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve datastores: %v", err)
	}

	for _, d := range datastores {
		ds := &Datastore{
			Moid:            d.Self.Value,
			Name:            d.Name,
			Type:            d.Summary.Type,
			Capacity:        d.Summary.Capacity,
			FreeSpace:       d.Summary.FreeSpace,
			Path:            d.Summary.Url,
			Accessible:      d.Summary.Accessible,
			MaintenanceMode: d.Summary.MaintenanceMode,
		}

		// 已使用空间和已置备空间
		ds.UsedSpace = ds.Capacity - ds.FreeSpace
		ds.ProvisionedSpace = ds.UsedSpace + d.Summary.Uncommitted

		// 关联主机 MoID
		for _, h := range d.Host {
			ds.HostMoids = append(ds.HostMoids, h.Key.Value)
		}

		// 关联虚拟机 MoID
		for _, vm := range d.Vm {
			ds.VmMoids = append(ds.VmMoids, vm.Value)
		}

		// 多主机访问
		if d.Summary.MultipleHostAccess != nil {
			ds.MultipleHostAccess = *d.Summary.MultipleHostAccess
		}

		// 存储 I/O 控制
		if d.IormConfiguration != nil {
			ds.StorageIOControl = d.IormConfiguration.Enabled
			if d.IormConfiguration.StatsCollectionEnabled != nil {
				ds.StatsCollectionEnabled = *d.IormConfiguration.StatsCollectionEnabled
			}
		}
		datastoreMap[ds.Moid] = ds
		// 日志打印
		// printDatastore(ds)
	}
	return datastoreMap, nil
}

func printDatastore(ds *Datastore) {
	// 打印数据中心信息
	log.Println("========= 数据存储信息 =========")
	log.Printf("Moid                 : %s", ds.Moid)
	log.Printf("Name                 : %s", ds.Name)
	log.Printf("Type                 : %s", ds.Type)
	log.Printf("Path                 : %s", ds.Path)
	log.Printf("Capacity             : %d", ds.Capacity)
	log.Printf("FreeSpace            : %d", ds.FreeSpace)
	log.Printf("UsedSpace            : %d", ds.UsedSpace)
	log.Printf("ProvisionedSpace     : %d", ds.ProvisionedSpace)
	log.Printf("Accessible           : %t", ds.Accessible)
	log.Printf("MaintenanceMode      : %s", ds.MaintenanceMode)
	log.Printf("MultipleHostAccess   : %t", ds.MultipleHostAccess)
	log.Printf("StorageIOControl     : %t", ds.StorageIOControl)
	log.Printf("StatsCollectionEnabled: %t", ds.StatsCollectionEnabled)
	log.Printf("HostMoids:")
	for _, h := range ds.HostMoids {
		log.Printf("  - %s", h)
	}
	log.Printf("VmMoids:")
	for _, v := range ds.VmMoids {
		log.Printf("  - %s", v)
	}
	log.Println("=================================")
}
