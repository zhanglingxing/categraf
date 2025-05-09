package horizon

import (
	"flashcat.cloud/categraf/types"
	"fmt"
	"log"
)

func (ins *Instance) connectionServersInfo(slist *types.SampleList, target, token string, labels map[string]string) {
	// 发送请求获取数据
	servers, err := GetRequest(ins, target+connectionServersURL, token)
	if err != nil {
		log.Printf("E! Failed to get connection servers data: %v", err)
		return
	}
	// 遍历所有层级数据，获取 connection servers 信息
	for _, connectionServer := range servers {
		// 提取字段
		csName, status, version, valid, validFrom, validTo, err := extractConnectionServerFields(connectionServer)
		if err != nil {
			log.Printf("W! %v", err)
			continue
		}
		// 创建标签
		connectionServersLabels := map[string]string{
			"csName":    csName,
			"status":    status,
			"version":   version,
			"valid":     valid,
			"validFrom": validFrom,
			"validTo":   validTo,
			"item_name": "connection_server_info",
		}
		// 设置全局标签
		for k, v := range labels {
			connectionServersLabels[k] = v
		}
		// 准备返回的数据（值为 1）
		fields := map[string]interface{}{
			"connection_server_info": 1,
		}
		// 发送数据
		slist.PushSamples(inputName, fields, connectionServersLabels)
	}

}

// extractConnectionServerFields 提取 connection server 的字段
func extractConnectionServerFields(server map[string]interface{}) (name, status, version, valid, validFrom, validTo string, err error) {
	// 提取 name
	name, ok := server["name"].(string)
	if !ok {
		return "", "", "", "", "", "", fmt.Errorf("missing or invalid 'name' field in jsonObject: %v", server)
	}

	// 提取 status
	status, ok = server["status"].(string)
	if !ok {
		return "", "", "", "", "", "", fmt.Errorf("missing or invalid 'status' field in jsonObject: %v", server)
	}

	// 提取 certificate
	certificate, ok := server["certificate"].(map[string]interface{})
	if !ok {
		return "", "", "", "", "", "", fmt.Errorf("missing or invalid 'certificate' field in jsonObject: %v", server)
	}

	// 提取 valid
	validBool, ok := certificate["valid"].(bool)
	if !ok {
		return "", "", "", "", "", "", fmt.Errorf("missing or invalid 'valid' field in certificate: %v", certificate)
	}
	valid = fmt.Sprintf("%v", validBool)

	// 提取 valid_from
	validFromInt, ok := certificate["valid_from"].(int64)
	if !ok {
		// 如果 valid_from 不是 int64，尝试解析为 float64 并转换为 int64
		validFromFloat, ok := certificate["valid_from"].(float64)
		if !ok {
			return "", "", "", "", "", "", fmt.Errorf("missing or invalid 'valid_from' field in certificate: %v", certificate)
		}
		validFromInt = int64(validFromFloat)
	}
	validFrom = fmt.Sprintf("%d", validFromInt) // 将 int64 转换为字符串

	// 提取 valid_to
	validToInt, ok := certificate["valid_to"].(int64)
	if !ok {
		// 如果 valid_to 不是 int64，尝试解析为 float64 并转换为 int64
		validToFloat, ok := certificate["valid_to"].(float64)
		if !ok {
			return "", "", "", "", "", "", fmt.Errorf("missing or invalid 'valid_to' field in certificate: %v", certificate)
		}
		validToInt = int64(validToFloat)
	}
	validTo = fmt.Sprintf("%d", validToInt) // 将 int64 转换为字符串

	// 提取 details
	details, ok := server["details"].(map[string]interface{})
	if !ok {
		return "", "", "", "", "", "", fmt.Errorf("missing or invalid 'details' field in jsonObject: %v", server)
	}

	// 提取 version
	version, ok = details["version"].(string)
	if !ok {
		return "", "", "", "", "", "", fmt.Errorf("missing or invalid 'version' field in details: %v", details)
	}

	return name, status, version, valid, validFrom, validTo, nil
}
