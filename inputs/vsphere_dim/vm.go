package vsphere_dim

import (
	"context"
	"flashcat.cloud/categraf/types"
	"fmt"
	"github.com/vmware/govmomi/vim25/mo"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"log"
	"strconv"
	"strings"
	"time"
)

// VM 虚拟机信息结构体
type VM struct {
	// 基础信息
	Moid            string // 虚拟机唯一 ID
	Name            string // 虚拟机名称
	PowerState      string // 电源状态
	ConnectionState string // 连接状态
	CreateTime      int64  // 创建时间（毫秒时间戳）
	UUID            string // 虚拟机 UUID

	// Guest 操作系统信息
	GuestID             string // 操作系统 ID
	GuestFamily         string // 操作系统家族
	GuestFullName       string // 操作系统完整名称
	GuestHostName       string // 主机名
	GuestIPAddress      string // IP 地址
	GuestDNSIPAddresses string // DNS IP 列表（逗号分隔）
	ToolsStatus         string // VMware Tools 状态
	ToolsVersion        string // VMware Tools 版本

	// 资源配置
	NumCPU         int32 // CPU 核心数
	MemorySizeMB   int32 // 内存大小（MB）
	MaxCPUUsage    int32 // 最大 CPU 使用量（MHz）
	MaxMemoryUsage int32 // 最大内存使用量（MB）

	// 存储信息
	StorageCommittedB   int64 // 已分配存储（B）
	StorageUncommittedB int64 // 未分配存储（B）
	StorageUnsharedB    int64 // 独享存储（B）
	TotalCapacityB      int64 // 总容量（B）
	TotalFreeSpaceB     int64 // 可用容量（B）

	// 显示与连接
	ScreenWidth       int32 // 屏幕宽度
	ScreenHeight      int32 // 屏幕高度
	BootTime          int64 // 启动时间（毫秒时间戳）
	NumMksConnections int32 // MKS 连接数

	// 其他
	ChangeVersion int64  // 配置版本（时间戳）
	HostMoid      string // 宿主机 ID
	// 存储信息
	DatastoreIDs []string // 用切片存储数据存储ID
}

// collectVM 收集虚拟机信息
func (ep *Endpoint) collectVM(ctx context.Context, slist *types.SampleList, ins *Instance) (map[string]*VM, error) {
	vmMap := make(map[string]*VM)
	var vms []mo.VirtualMachine

	// 查询虚拟机信息
	err := ep.QueryObjects(ctx, "VirtualMachine", []string{
		"name", "config", "guest", "runtime", "summary", "datastore",
	}, &vms)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve VMs: %v", err)
	}

	for _, vmo := range vms {
		vm := buildVMInfo(&vmo)
		vmMap[vm.Moid] = vm
		// 打印虚拟机信息
		//printVM(vm)
		//labels := buildVMLabels(vm, ins.Labels)
		//slist.PushSample(inputName, "vm_info", 1, labels)
	}

	return vmMap, nil
}

// buildVMInfo 构建虚拟机信息对象
func buildVMInfo(vm *mo.VirtualMachine) *VM {
	info := &VM{
		Moid: safeString(vm.Self.Value),
		Name: safeString(vm.Name),
	}

	// 运行状态信息
	info.PowerState = string(vm.Runtime.PowerState)
	info.ConnectionState = string(vm.Runtime.ConnectionState)
	info.MaxCPUUsage = vm.Runtime.MaxCpuUsage
	info.MaxMemoryUsage = vm.Runtime.MaxMemoryUsage
	info.BootTime = convertTime(vm.Runtime.BootTime)
	info.NumMksConnections = vm.Runtime.NumMksConnections
	if vm.Runtime.Host != nil {
		info.HostMoid = vm.Runtime.Host.Value
	}

	// 配置信息
	if vm.Config != nil {
		info.UUID = safeString(vm.Config.Uuid)
		info.CreateTime = convertTime(vm.Config.CreateDate)
		info.ChangeVersion = convertToTimestamp(vm.Config.ChangeVersion)
	}

	// 概要配置信息
	info.NumCPU = vm.Summary.Config.NumCpu
	info.MemorySizeMB = vm.Summary.Config.MemorySizeMB
	if vm.Summary.Storage != nil {
		info.StorageCommittedB = vm.Summary.Storage.Committed
		info.StorageUncommittedB = vm.Summary.Storage.Uncommitted
		info.StorageUnsharedB = vm.Summary.Storage.Unshared
	}

	// Guest 信息
	if vm.Guest != nil {
		info.GuestID = safeString(vm.Guest.GuestId)
		info.GuestFamily = safeString(vm.Guest.GuestFamily)
		info.GuestFullName = safeString(vm.Guest.GuestFullName)
		info.GuestHostName = safeString(vm.Guest.HostName)
		info.GuestIPAddress = safeString(vm.Guest.IpAddress)
		info.ToolsStatus = string(vm.Guest.ToolsStatus)
		info.ToolsVersion = safeString(vm.Guest.ToolsVersion)

		if vm.Guest.Screen != nil {
			info.ScreenWidth = vm.Guest.Screen.Width
			info.ScreenHeight = vm.Guest.Screen.Height
		}

		info.GuestDNSIPAddresses = strings.Join(collectGuestDNS(vm.Guest), ",")
		info.TotalCapacityB, info.TotalFreeSpaceB = collectGuestDiskCapacity(vm.Guest)
	}

	// 获取数据存储信息
	if len(vm.Datastore) > 0 {
		datastoreIDs := make([]string, 0, len(vm.Datastore))
		for _, ds := range vm.Datastore {
			datastoreIDs = append(datastoreIDs, ds.Value)
		}
		info.DatastoreIDs = datastoreIDs
	}

	return info
}

// buildVMLabels 构建虚拟机的指标标签
func buildVMLabels(vm *VM, globalLabels map[string]string) map[string]string {
	labels := map[string]string{
		"vm_moid":               vm.Moid,
		"name":                  vm.Name,
		"power_state":           vm.PowerState,
		"connection_state":      vm.ConnectionState,
		"create_time":           strconv.FormatInt(vm.CreateTime, 10),
		"uuid":                  vm.UUID,
		"guest_id":              vm.GuestID,
		"guest_family":          vm.GuestFamily,
		"guest_full_name":       vm.GuestFullName,
		"guest_host_name":       vm.GuestHostName,
		"guest_ip_address":      vm.GuestIPAddress,
		"guest_dns_ips":         vm.GuestDNSIPAddresses,
		"tools_status":          vm.ToolsStatus,
		"tools_version":         vm.ToolsVersion,
		"num_cpu":               strconv.FormatInt(int64(vm.NumCPU), 10),
		"memory_size_mb":        strconv.FormatInt(int64(vm.MemorySizeMB), 10),
		"max_cpu_usage":         strconv.FormatInt(int64(vm.MaxCPUUsage), 10),
		"max_memory_usage":      strconv.FormatInt(int64(vm.MaxMemoryUsage), 10),
		"storage_committed_b":   strconv.FormatInt(vm.StorageCommittedB, 10),
		"storage_uncommitted_b": strconv.FormatInt(vm.StorageUncommittedB, 10),
		"storage_unshared_b":    strconv.FormatInt(vm.StorageUnsharedB, 10),
		"total_capacity_b":      strconv.FormatInt(vm.TotalCapacityB, 10),
		"total_free_space_b":    strconv.FormatInt(vm.TotalFreeSpaceB, 10),
		"screen_width":          strconv.FormatInt(int64(vm.ScreenWidth), 10),
		"screen_height":         strconv.FormatInt(int64(vm.ScreenHeight), 10),
		"boot_time":             strconv.FormatInt(vm.BootTime, 10),
		"num_mks_connections":   strconv.FormatInt(int64(vm.NumMksConnections), 10),
		"change_version":        strconv.FormatInt(vm.ChangeVersion, 10),
		"host_moid":             vm.HostMoid,
		"datastore_ids":         strings.Join(vm.DatastoreIDs, ","),
	}

	for k, v := range globalLabels {
		labels[k] = v
	}

	return labels
}

// collectGuestDNS 收集 guest 网络 DNS 地址
func collectGuestDNS(guest *vimtypes.GuestInfo) []string {
	var dnsAddresses []string
	if guest != nil && guest.Net != nil {
		for _, nic := range guest.Net {
			if nic.IpAddress != nil {
				dnsAddresses = append(dnsAddresses, nic.IpAddress...)
			}
		}
	}
	return dnsAddresses
}

// collectGuestDiskCapacity 收集 guest 磁盘容量信息
func collectGuestDiskCapacity(guest *vimtypes.GuestInfo) (int64, int64) {
	var totalCapacity, totalFreeSpace int64
	if guest != nil && guest.Disk != nil {
		for _, disk := range guest.Disk {
			totalCapacity += disk.Capacity
			totalFreeSpace += disk.FreeSpace
		}
	}
	return totalCapacity, totalFreeSpace
}

// collectDatastoreIDs 提取数据存储ID列表
func collectDatastoreIDs(datastores []vimtypes.ManagedObjectReference) []string {
	var ids []string
	for _, ds := range datastores {
		if ds.Value != "" {
			ids = append(ids, ds.Value)
		}
	}
	return ids
}

// convertTime 时间转换为毫秒时间戳
func convertTime(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.UnixNano() / int64(time.Millisecond)
}

// safeString 安全返回字符串
func safeString(s string) string {
	return s
}

// convertToTimestamp 将 RFC3339 格式字符串转为时间戳（毫秒）
func convertToTimestamp(changeVersion string) int64 {
	t, err := time.Parse(time.RFC3339, changeVersion)
	if err != nil {
		return 0
	}
	return t.UnixMilli()
}

func printVM(vm *VM) {
	log.Println("========= 虚拟机信息 =========")
	log.Printf("%-30s: %s", "Moid (虚拟机唯一ID)", vm.Moid)
	log.Printf("%-30s: %s", "Name (虚拟机名称)", vm.Name)
	log.Printf("%-30s: %s", "PowerState (电源状态)", vm.PowerState)
	log.Printf("%-30s: %s", "ConnectionState (连接状态)", vm.ConnectionState)
	log.Printf("%-30s: %d", "CreateTime (创建时间)", vm.CreateTime)
	log.Printf("%-30s: %s", "UUID (唯一标识符)", vm.UUID)

	log.Printf("%-30s: %s", "GuestID (操作系统ID)", vm.GuestID)
	log.Printf("%-30s: %s", "GuestFamily (操作系统家族)", vm.GuestFamily)
	log.Printf("%-30s: %s", "GuestFullName (完整操作系统名)", vm.GuestFullName)
	log.Printf("%-30s: %s", "GuestHostName (主机名)", vm.GuestHostName)
	log.Printf("%-30s: %s", "GuestIPAddress (IP地址)", vm.GuestIPAddress)
	log.Printf("%-30s: %s", "GuestDNSIPAddresses (DNS列表)", vm.GuestDNSIPAddresses)
	log.Printf("%-30s: %s", "ToolsStatus (VMware Tools 状态)", vm.ToolsStatus)
	log.Printf("%-30s: %s", "ToolsVersion (VMware Tools 版本)", vm.ToolsVersion)

	log.Printf("%-30s: %d", "NumCPU (CPU核数)", vm.NumCPU)
	log.Printf("%-30s: %d", "MemorySizeMB (内存大小MB)", vm.MemorySizeMB)
	log.Printf("%-30s: %d", "MaxCPUUsage (最大CPU使用量MHz)", vm.MaxCPUUsage)
	log.Printf("%-30s: %d", "MaxMemoryUsage (最大内存使用量MB)", vm.MaxMemoryUsage)

	log.Printf("%-30s: %d", "StorageCommittedB (已分配存储B)", vm.StorageCommittedB)
	log.Printf("%-30s: %d", "StorageUncommittedB (未分配存储B)", vm.StorageUncommittedB)
	log.Printf("%-30s: %d", "StorageUnsharedB (独享存储B)", vm.StorageUnsharedB)
	log.Printf("%-30s: %d", "TotalCapacityB (总容量B)", vm.TotalCapacityB)
	log.Printf("%-30s: %d", "TotalFreeSpaceB (可用容量B)", vm.TotalFreeSpaceB)

	log.Printf("%-30s: %d", "ScreenWidth (屏幕宽度)", vm.ScreenWidth)
	log.Printf("%-30s: %d", "ScreenHeight (屏幕高度)", vm.ScreenHeight)
	log.Printf("%-30s: %d", "BootTime (启动时间)", vm.BootTime)
	log.Printf("%-30s: %d", "NumMksConnections (MKS连接数)", vm.NumMksConnections)

	log.Printf("%-30s: %d", "ChangeVersion (变更版本时间戳)", vm.ChangeVersion)
	log.Printf("%-30s: %s", "HostMoid (宿主机ID)", vm.HostMoid)
	log.Printf("%-30s: %s", "DatastoreIDs (数据存储IDs)", vm.DatastoreIDs)
	log.Println("=================================")
}
