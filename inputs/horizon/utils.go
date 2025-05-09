package horizon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

func GetToken(ins *Instance, target, url string) (string, error) {
	// 构建登录请求体
	data := map[string]string{
		"username": ins.Username,
		"password": ins.Password,
		"domain":   ins.Domain,
	}
	// 将数据转换为 JSON
	loginJSON, err := json.Marshal(data)
	if err != nil {
		log.Println("E! failed to marshal login data:", err)
		return "", err
	}
	// 创建请求
	req, err := http.NewRequest("POST", target+url, bytes.NewBuffer(loginJSON))
	if err != nil {
		log.Println("E! failed to create request:", err)
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	// 发送请求
	resp, err := ins.client.Do(req)
	if err != nil {
		log.Println("E! request failed:", err)
		return "", err
	}
	defer resp.Body.Close()
	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		log.Println("E! unexpected status code:", resp.StatusCode)
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	// 解析响应体
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Println("E! failed to decode response body:", err)
		return "", err
	}
	// 提取 token
	if token, ok := result["access_token"].(string); ok {
		return token, nil
	}
	log.Println("E! access_token not found in response")
	return "", fmt.Errorf("access_token not found in response")
}

func GetRequest(ins *Instance, url, token string) ([]map[string]interface{}, error) {
	// 创建请求
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Println("E! failed to create request:", err)
		return nil, err
	}
	// 设置授权头
	req.Header.Set("Authorization", "Bearer "+token)
	// 发送请求
	resp, err := ins.client.Do(req)
	if err != nil {
		log.Println("E! request failed:", err)
		return nil, err
	}
	defer resp.Body.Close()
	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		log.Println("E! unexpected status code:", resp.StatusCode, " url:", url)
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	// 读取响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("E! failed to read response body:", err)
		return nil, err
	}
	// 解析 JSON 响应数据
	var result []map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		log.Println("E! failed to decode response body:", err)
		return nil, err
	}
	return result, nil
}

func GetPaginatedRequest(ins *Instance, url, token string, size int) ([]map[string]interface{}, error) {
	var allResults []map[string]interface{}
	page := 1
	for {
		// 构造带分页参数的 URL
		paginatedURL := fmt.Sprintf("%s?page=%d&size=%d", url, page, size)
		// log.Println("I! horizon pagination query:", paginatedURL)
		// 创建请求
		req, err := http.NewRequest("GET", paginatedURL, nil)
		if err != nil {
			log.Println("E! failed to create request:", err)
			return nil, err
		}
		// 设置授权头
		req.Header.Set("Authorization", "Bearer "+token)
		// 发送请求
		resp, err := ins.client.Do(req)
		if err != nil {
			log.Println("E! request failed:", err)
			return nil, err
		}
		defer resp.Body.Close()

		// 检查响应状态 - 如果是第二页及之后的页面返回 BAD_REQUEST，认为数据已全部获取
		if resp.StatusCode != http.StatusOK {
			if page > 1 && resp.StatusCode == http.StatusBadRequest {
				break // 跳出循环
			}
			return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		// 读取响应体
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Println("E! failed to read response body:", err)
			return nil, err
		}
		// 解析 JSON 响应数据
		var result []map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			log.Println("E! failed to decode response body:", err)
			return nil, err
		}
		// 将结果添加到总集合中
		allResults = append(allResults, result...)
		// 如果当前页返回的结果小于请求的 size，则说明数据已全部获取
		if len(result) < size {
			break
		}
		page++
	}
	return allResults, nil
}
