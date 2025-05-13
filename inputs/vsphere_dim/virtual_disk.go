package vsphere_dim

import (
	"context"
	"flashcat.cloud/categraf/types"
	"fmt"
	"github.com/vmware/govmomi/vim25/mo"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"log"
	"math"
)

// VirtualDisk 结构体，只保留 DiskFilePath 和 SizeGB
type VirtualDisk struct {
	VmMoid          string
	DatastoreMoid   string
	VirtualDiskName string
	CapacityGB      int32
	DiskFilePath    string // 磁盘文件路径
}

// collectVirtualDisk 收集虚拟磁盘信息
func (ep *Endpoint) collectVirtualDisk(ctx context.Context, slist *types.SampleList, ins *Instance) ([]*VirtualDisk, error) {
	var virtualDiskList []*VirtualDisk

	var vms []mo.VirtualMachine
	err := ep.QueryObjects(ctx, "VirtualMachine", []string{
		"config.hardware.device",
		"summary",
	}, &vms)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve VMs: %v", err)
	}

	for _, vm := range vms {
		vmMoid := vm.Self.Value

		// 遍历设备
		for _, device := range vm.Config.Hardware.Device {
			disk, ok := device.(*vimtypes.VirtualDisk)
			if !ok {
				continue
			}

			// 生成虚拟磁盘名称，比如 scsi0:0
			controllerKey := disk.ControllerKey
			unitNumber := int32(-1)
			if disk.UnitNumber != nil {
				unitNumber = *disk.UnitNumber
			}
			virtualDiskName := fmt.Sprintf("scsi%d:%d", controllerKey-1000, unitNumber)

			// 计算磁盘容量（单位GB）
			diskCapacityGB := int32(math.Ceil(float64(disk.CapacityInBytes) / (1024 * 1024 * 1024)))

			// 获取磁盘所在的Datastore Moid
			datastoreMoid := ""
			if disk.Backing != nil {
				if backingInfo, ok := disk.Backing.(*vimtypes.VirtualDiskFlatVer2BackingInfo); ok && backingInfo.Datastore != nil {
					datastoreMoid = backingInfo.Datastore.Value
				}
			}

			// 获取磁盘文件路径
			diskFilePath := ""
			if disk.Backing != nil {
				if backingInfo, ok := disk.Backing.(*vimtypes.VirtualDiskFlatVer2BackingInfo); ok {
					diskFilePath = backingInfo.FileName
				}
			}

			// 生成 VirtualDisk 对象
			vd := &VirtualDisk{
				VmMoid:          vmMoid,
				DatastoreMoid:   datastoreMoid,
				VirtualDiskName: virtualDiskName,
				CapacityGB:      diskCapacityGB,
				DiskFilePath:    diskFilePath,
			}
			virtualDiskList = append(virtualDiskList, vd)
			// 打印日志
			//printVirtualDisk(vd)
		}
	}
	return virtualDiskList, nil
}
func printVirtualDisk(vd *VirtualDisk) {
	// 打印数据中心信息
	log.Printf("虚拟磁盘:")
	log.Printf("VM Moid: %s", vd.VmMoid)
	log.Printf("Datastore Moid: %s", vd.DatastoreMoid)
	log.Printf("Virtual Disk Name: %s", vd.VirtualDiskName)
	log.Printf("Capacity (GB): %d", vd.CapacityGB)
	log.Printf("Disk File Path: %s", vd.DiskFilePath)
	log.Printf("===========================")
}
