package horizon

import (
	"flashcat.cloud/categraf/types"
	"fmt"
	"log"
	"sort"
	"strings"
)

type VCenterInfo struct {
	ID          string
	Datacenters []DatacenterInfo
}

type DatacenterInfo struct {
	ID             string
	HostOrClusters []string
}

func (ins *Instance) gatherDatastoresInfo(slist *types.SampleList, target, token string, labels map[string]string) {
	// 获取所有层级数据
	vcenters, err := getAllHierarchyData(ins, target, token)
	if err != nil {
		log.Printf("E! %v", err)
		return
	}
	// 使用 map 来去重，key 是组合的唯一标识
	uniqueDatastores := make(map[string]struct{})

	// 遍历所有层级数据，获取 datastores 信息
	for _, vcenter := range vcenters {
		for _, datacenter := range vcenter.Datacenters {
			for _, hostOrClusterID := range datacenter.HostOrClusters {
				datastoresData, err := getDatastores(ins, target, token, vcenter.ID, datacenter.ID, hostOrClusterID)
				if err != nil {
					log.Printf("E! Failed to get datastores for vcenter_id %s, datacenter_id %s, "+
						"host_or_cluster_id %s: %v", vcenter.ID, datacenter.ID, hostOrClusterID, err)
					continue
				}
				// 解析并去重数据
				for _, datastore := range datastoresData {
					name, nameOk := datastore["name"].(string)
					localDatastore, localOk := datastore["local_datastore"].(bool)

					if !nameOk || !localOk {
						log.Printf("W! Missing or invalid fields in datastore: %v", datastore)
						continue
					}
					// 创建标签
					datastoreLabels := map[string]string{
						"datastoreName":  name,
						"item_name":      "datastore_local",
						"datastoreLocal": fmt.Sprintf("%v", localDatastore),
					}
					// 合并全局标签
					for k, v := range labels {
						datastoreLabels[k] = v
					}
					// 生成组合的唯一键
					key := generateUniqueKey(datastoreLabels)
					// 如果组合键已经存在，跳过
					if _, exists := uniqueDatastores[key]; exists {
						continue
					}
					// 将组合键存入 map，标记为已处理
					uniqueDatastores[key] = struct{}{}
					// 准备返回的数据（值为 1）
					fields := map[string]interface{}{
						"datastore_local": 1,
					}
					// 把数据推送到 slist
					slist.PushSamples(inputName, fields, datastoreLabels)
				}
			}
		}
	}
}

// 生成组合的唯一键
func generateUniqueKey(labels map[string]string) string {
	// 将 labels 按 key 排序，确保顺序一致
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// 拼接所有键值对
	var keyBuilder strings.Builder
	for _, k := range keys {
		keyBuilder.WriteString(k)
		keyBuilder.WriteString("=")
		keyBuilder.WriteString(labels[k])
		keyBuilder.WriteString(";")
	}
	return keyBuilder.String()
}

// 获取所有 vcenter_id、datacenter_id 和 host_or_cluster_id
func getAllHierarchyData(ins *Instance, target, token string) ([]VCenterInfo, error) {
	vcenterIDs, err := getVcenterIDs(ins, target, token)
	if err != nil {
		return nil, fmt.Errorf("failed to get vcenter_ids: %v", err)
	}
	var vcenters []VCenterInfo
	for _, vcenterID := range vcenterIDs {
		vcenter := VCenterInfo{
			ID: vcenterID,
		}
		datacenterIDs, err := getDatacenterIDs(ins, target, token, vcenterID)
		if err != nil {
			log.Printf("W! Failed to get datacenter_ids for vcenter_id %s: %v", vcenterID, err)
			continue
		}
		for _, datacenterID := range datacenterIDs {
			datacenter := DatacenterInfo{
				ID: datacenterID,
			}
			hostOrClusterIDs, err := getHostOrClusterIDs(ins, target, token, vcenterID, datacenterID)
			if err != nil {
				log.Printf("W! Failed to get host_or_cluster_ids for vcenter_id %s, datacenter_id %s: %v", vcenterID, datacenterID, err)
				continue
			}
			datacenter.HostOrClusters = hostOrClusterIDs
			vcenter.Datacenters = append(vcenter.Datacenters, datacenter)
		}
		vcenters = append(vcenters, vcenter)
	}
	if len(vcenters) == 0 {
		return nil, fmt.Errorf("no valid vcenters found")
	}
	return vcenters, nil
}

// 获取所有 vcenter_id
func getVcenterIDs(ins *Instance, target, token string) ([]string, error) {
	url := target + centersURL
	return getIDs(ins, url, token, "id")
}

// 获取所有 datacenter_id
func getDatacenterIDs(ins *Instance, target, token, vcenterID string) ([]string, error) {
	url := fmt.Sprintf("%s"+datacentersURL+"?vcenter_id=%s", target, vcenterID)
	return getIDs(ins, url, token, "id")
}

// 获取所有 host_or_cluster_id
func getHostOrClusterIDs(ins *Instance, target, token, vcenterID, datacenterID string) ([]string, error) {
	url := fmt.Sprintf("%s"+hostsOrClustersURL+"?vcenter_id=%s&datacenter_id=%s", target, vcenterID, datacenterID)
	return getIDs(ins, url, token, "id")
}

// 获取 datastores 信息
func getDatastores(ins *Instance, target, token, vcenterID, datacenterID, hostOrClusterID string) ([]map[string]interface{}, error) {
	url := fmt.Sprintf("%s"+datastoresURL+"?vcenter_id=%s&datacenter_id=%s&host_or_cluster_id=%s",
		target, vcenterID, datacenterID, hostOrClusterID)
	datastores, err := GetRequest(ins, url, token)
	if err != nil {
		return nil, err
	}
	return datastores, nil
}

// 通用的获取 ID 列表的方法
func getIDs(ins *Instance, url, token, idField string) ([]string, error) {
	data, err := GetRequest(ins, url, token)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("no data found for URL: %s", url)
	}
	var ids []string
	for _, item := range data {
		id, ok := item[idField].(string)
		if !ok {
			log.Printf("W! Invalid %s format: %v", idField, item)
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}
