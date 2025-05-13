package vsphere_dim

import (
	"context"
	"flashcat.cloud/categraf/types"
	"fmt"
	"github.com/vmware/govmomi/vim25/mo"
	"log"
)

// Datacenter 数据中心信息结构体
type Datacenter struct {
	Name string // 数据中心名称
	Moid string // 数据中心唯一 ID
}

// collectDatacenter 收集数据中心信息
func (ep *Endpoint) collectDatacenter(ctx context.Context, slist *types.SampleList, ins *Instance) (map[string]*Datacenter, error) {
	datacenterMap := make(map[string]*Datacenter)
	var datacenters []mo.Datacenter

	// 查询数据中心信息
	err := ep.QueryObjects(ctx, "Datacenter", []string{
		"name",
		"parent",
	}, &datacenters)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve datacenters: %v", err)
	}

	for _, dc := range datacenters {
		datacenter := &Datacenter{
			Name: dc.Name,
			Moid: dc.Self.Value,
		}
		// 打印数据中心信息
		// printDatacenter(datacenter)
		datacenterMap[datacenter.Moid] = datacenter
	}
	return datacenterMap, nil
}

func printDatacenter(datacenter *Datacenter) {
	// 打印数据中心信息
	log.Println("========= 数据中心信息 =========")
	log.Printf("%-30s: %s", "Name (数据中心名称)", datacenter.Name)
	log.Printf("%-30s: %s", "Moid (数据中心唯一ID)", datacenter.Moid)
	log.Println("=================================")
}
