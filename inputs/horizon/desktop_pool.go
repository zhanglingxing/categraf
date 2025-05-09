package horizon

import (
	"flashcat.cloud/categraf/types"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DesktopPool 结构体定义桌面池的相关信息
type DesktopPool struct {
	ID                       string
	Name                     string
	Enabled                  string
	Type                     string
	AuthorizedUserCount      int
	SessionCount             int
	ActiveSessionCount       int
	ConnectedSessionCount    int
	DisconnectedSessionCount int
	MachineCount             int
	ConnectedMachineCount    int
	ProblematicMachineCount  int
	ActiveMachineCount       int
}

// Horizon 定义一个结构体来存储聚合后的值
type Horizon struct {
	AuthorizedUserCount      int
	SessionCount             int
	ActiveSessionCount       int
	ConnectedSessionCount    int
	DisconnectedSessionCount int
	MachineCount             int
	ConnectedMachineCount    int
	ActiveMachineCount       int
	ProblematicMachineCount  int
	ProblematicDeskPoolCount int
	ActiveDeskPoolCount      int
	DeskPoolCount            int
}

type SessionData struct {
	SessionId         string
	SessionState      string
	MachineId         string
	DesktopPoolId     string
	StartTime         int64
	SessionDurationMs int64
	IdleDuration      int64
	SessionProtocol   string
	ClientIP          string
	ClientName        string
	UserId            string
	BrokerUserId      string
	LogonOutTimeMs    int64
}

type MachineData struct {
	MachineId        string
	MachineName      string
	UserName         string
	HostName         string
	ConnectionServer string
}

type MachineCountStats struct {
	MachineCount            int
	ConnectedMachineCount   int
	ProblematicMachineCount int
	ActiveMachineCount      int
}

type AdUsersOrGroup struct {
	Id                string
	Domain            string
	LoginName         string
	UserPrincipalName string
}

// 新增方法获取缓存的机器数据（同时返回两个结果）
func (ins *Instance) getCachedMachineResults(target, token string) (map[string]MachineCountStats, map[string]MachineData) {
	ins.machineCache.mutex.Lock()
	defer ins.machineCache.mutex.Unlock()
	// 如果缓存为空或超过2小时未更新，则重新获取
	if ins.machineCache.machineData == nil || time.Since(ins.machineCache.timestamp) > 2*time.Hour {
		countStats, machineData, err := getDesktopPoolMachines(ins, target, token)
		if err != nil {
			log.Printf("E! Failed to update machine cache: %v", err)
			// 返回现有缓存，即使可能已过期
			return ins.machineCache.countStats, ins.machineCache.machineData
		}
		// 更新两个缓存
		ins.machineCache.countStats = countStats
		ins.machineCache.machineData = machineData
		ins.machineCache.timestamp = time.Now()
	}
	return ins.machineCache.countStats, ins.machineCache.machineData
}

// getDesktopPool 获取桌面池的所有相关数据，并推送到 slist
func (ins *Instance) getDesktopPool(slist *types.SampleList, target, token string, labels map[string]string) {
	var (
		desktopPoolResult = make(map[string]*DesktopPool)
		mu                sync.Mutex
		wg                sync.WaitGroup
		sessionResult     = make(map[string]SessionData)
	)

	// 从缓存获取 machineCountStats 和 machineResult
	countStats, machineResult := ins.getCachedMachineResults(target, token)

	// 填充桌面池的机器统计信息
	for id, stats := range countStats {
		if _, exists := desktopPoolResult[id]; !exists {
			desktopPoolResult[id] = &DesktopPool{ID: id}
		}
		desktopPoolResult[id].MachineCount = stats.MachineCount
		desktopPoolResult[id].ConnectedMachineCount = stats.ConnectedMachineCount
		desktopPoolResult[id].ProblematicMachineCount = stats.ProblematicMachineCount
		desktopPoolResult[id].ActiveMachineCount = stats.ActiveMachineCount
	}

	// 定义所有并发执行的函数
	fetchFunctions := []func(){
		// 获取桌面池授权信息
		func() {
			defer wg.Done()
			if entitlements, err := getDesktopPoolEntitlements(ins, target, token); err == nil {
				mu.Lock()
				for id, count := range entitlements {
					if _, exists := desktopPoolResult[id]; !exists {
						desktopPoolResult[id] = &DesktopPool{ID: id}
					}
					desktopPoolResult[id].AuthorizedUserCount = count
				}
				mu.Unlock()
			} else {
				log.Printf("E! Failed to get desktop pool entitlements: %v", err)
			}
		},
		// 获取桌面池基本信息
		func() {
			defer wg.Done()
			if inventory, err := getDesktopPoolInventory(ins, target, token); err == nil {
				mu.Lock()
				for id, poolInfo := range inventory {
					if _, exists := desktopPoolResult[id]; !exists {
						desktopPoolResult[id] = &DesktopPool{ID: id}
					}
					desktopPoolResult[id].Name = poolInfo.Name
					desktopPoolResult[id].Enabled = poolInfo.Enabled
					desktopPoolResult[id].Type = poolInfo.Type
				}
				mu.Unlock()
			} else {
				log.Printf("E! Failed to get desktop pool inventory: %v", err)
			}
		},
		// 获取桌面池会话信息
		func() {
			defer wg.Done()
			if sessions, result, err := getDesktopPoolSessions(ins, target, token); err == nil {
				mu.Lock()
				for id, sessionData := range sessions {
					if _, exists := desktopPoolResult[id]; !exists {
						desktopPoolResult[id] = &DesktopPool{ID: id}
					}
					desktopPoolResult[id].SessionCount = sessionData.SessionCount
					desktopPoolResult[id].ActiveSessionCount = sessionData.ActiveSessionCount
					desktopPoolResult[id].ConnectedSessionCount = sessionData.ConnectedSessionCount
					desktopPoolResult[id].DisconnectedSessionCount = sessionData.DisconnectedSessionCount
				}
				// 正确合并 sessionResult
				for sessionId, data := range result {
					sessionResult[sessionId] = data
				}
				mu.Unlock()
			} else {
				log.Printf("E! Failed to get desktop pool sessions: %v", err)
			}
		},
		// 获取桌面池机器信息
		/*func() {
			defer wg.Done()
			if machineCountStats, machineData, err := getDesktopPoolMachines(ins, target, token); err == nil {
				mu.Lock()
				for id, machineData := range machineCountStats {
					if _, exists := desktopPoolResult[id]; !exists {
						desktopPoolResult[id] = &DesktopPool{ID: id}
					}
					desktopPoolResult[id].MachineCount = machineData.MachineCount
					desktopPoolResult[id].ConnectedMachineCount = machineData.ConnectedMachineCount
					desktopPoolResult[id].ProblematicMachineCount = machineData.ProblematicMachineCount
					desktopPoolResult[id].ActiveMachineCount = machineData.ActiveMachineCount
				}
				// 正确合并 machineResult
				for machineId, data := range machineData {
					machineResult[machineId] = data
				}
				mu.Unlock()
			} else {
				log.Printf("E! Failed to get desktop pool machines: %v", err)
			}
		},
		// 获取 ad-users-or-groups 用户相关的数据
		func() {
			defer wg.Done()
			if result, err := getAdUsersOrGroups(ins, target, token); err == nil {
				mu.Lock()
				// 正确合并 adUsersOrGroupResult
				for id, data := range result {
					adUsersOrGroupResult[id] = data
				}
				mu.Unlock()
			} else {
				log.Printf("E! Failed to get desktop pool entitlements: %v", err)
			}
		},*/
	}

	// 启动所有 Goroutines 进行并发数据拉取
	wg.Add(len(fetchFunctions))
	for _, fetchFunc := range fetchFunctions {
		go fetchFunc()
	}
	wg.Wait()

	// 推送数据到categraf框架
	PushData(desktopPoolResult, slist, labels)

	// 推送 session 数据到 categraf 框架
	ProcessAndSendSessionData(ins, sessionResult, machineResult, desktopPoolResult, slist, labels)
}

func ProcessAndSendSessionData(ins *Instance,
	sessionResult map[string]SessionData,
	machineResult map[string]MachineData,
	desktopPoolResult map[string]*DesktopPool,
	slist *types.SampleList, labels map[string]string) {

	for _, session := range sessionResult {
		// 获取桌面池信息
		pool, exists := desktopPoolResult[session.DesktopPoolId]
		if !exists {
			log.Printf("E! Desktop Pool ID not found: %s\n", session.DesktopPoolId)
			continue
		}
		// 获取机器信息
		machine, exists := machineResult[session.MachineId]
		if !exists {
			log.Printf("E! Machine ID not found: %s\n", session.MachineId)
			continue
		}

		// 组装标签
		sessionLabels := map[string]string{
			"desktopPoolName": pool.Name,
			"machineName":     machine.MachineName,
			"vdi":             machine.MachineName,
			"vcHostName":      machine.HostName,
			"csName":          machine.ConnectionServer,
			"userSessionId":   session.SessionId,
			"sessionStatus":   session.SessionState,
			"loginTime":       strconv.FormatInt(session.StartTime, 10),
			"item_name":       inputName + "_user_session_info",
			"doMain":          ins.Domain,
		}

		if machine.UserName != "" {
			sessionLabels["userSessionName"] = machine.UserName
			sessionLabels["fullName"] = machine.UserName
		}
		if session.LogonOutTimeMs != 0 {
			sessionLabels["logonOutTimeMs"] = strconv.FormatInt(session.LogonOutTimeMs, 10)
		}

		if session.SessionDurationMs != 0 {
			sessionLabels["sessionDurationMs"] = strconv.FormatInt(session.SessionDurationMs, 10)
		}
		if session.IdleDuration != 0 {
			sessionLabels["idleDuration"] = strconv.FormatInt(session.IdleDuration, 10)
		}
		if session.SessionProtocol != "" {
			sessionLabels["sessionProtocol"] = session.SessionProtocol
		}
		if session.ClientIP != "" {
			sessionLabels["clientIp"] = session.ClientIP
		}
		if session.ClientName != "" {
			sessionLabels["clientName"] = session.ClientName
		}

		// 添加全局 labels
		for k, v := range labels {
			sessionLabels[k] = v
		}
		// 组装字段
		fields := map[string]interface{}{
			"user_session_info": 1,
		}
		// 推送数据
		slist.PushSamples(inputName, fields, sessionLabels)

		// 推送数据到 Kafka
		if ins.KafkaProducer != nil {
			if err := ins.KafkaProducer.SendMessage(fields, sessionLabels, ins.Labels, ins.CurrentTimestamp); err != nil {
				log.Printf("E! Failed to send data to Kafka: %v", err)
			}
		}
	}
}

// 获取 ad-users-or-groups 用户相关的数据
/*func getAdUsersOrGroups(ins *Instance, target, token string) (map[string]AdUsersOrGroup, error) {
	// 发送请求获取数据
	adUsersOrGroups, err := GetPaginatedRequest(ins, target+adUsersOrGroupsURL, token, 1000)
	if err != nil {
		return nil, err
	}
	// 打印获取到的数据大小
	log.Printf("获取到的 adUsersOrGroups-1 数据量: %d\n", len(adUsersOrGroups))
	// 结果存储
	result := make(map[string]AdUsersOrGroup)
	// 遍历数据
	for _, adUsersOrGroupParam := range adUsersOrGroups {
		adUsersOrGroup := AdUsersOrGroup{}
		if domain, ok := adUsersOrGroupParam["domain"].(string); ok {
			adUsersOrGroup.Domain = domain
		}
		if loginName, ok := adUsersOrGroupParam["login_name"].(string); ok {
			adUsersOrGroup.LoginName = loginName
		}
		if userPrincipalName, ok := adUsersOrGroupParam["user_principal_name"].(string); ok {
			adUsersOrGroup.UserPrincipalName = userPrincipalName
		}
		if id, ok := adUsersOrGroupParam["id"].(string); ok {
			adUsersOrGroup.Id = id
			result[id] = adUsersOrGroup
		}
	}
	// 打印处理后结果的大小
	log.Printf("处理后的结果 adUsersOrGroups-2 数据量: %d\n", len(result))
	return result, nil
}*/

// PushData 封装数据聚合和推送的逻辑
func PushData(desktopPoolResult map[string]*DesktopPool, slist *types.SampleList, labels map[string]string) {
	// 初始化聚合结果
	aggregatedHorizon := Horizon{}

	// 遍历 desktopPoolResult，累加字段值并推送到 slist
	for _, pool := range desktopPoolResult {
		// 累加字段值
		aggregatedHorizon.AuthorizedUserCount += pool.AuthorizedUserCount
		aggregatedHorizon.SessionCount += pool.SessionCount
		aggregatedHorizon.ActiveSessionCount += pool.ActiveSessionCount
		aggregatedHorizon.ConnectedSessionCount += pool.ConnectedSessionCount
		aggregatedHorizon.DisconnectedSessionCount += pool.DisconnectedSessionCount
		aggregatedHorizon.MachineCount += pool.MachineCount
		aggregatedHorizon.ConnectedMachineCount += pool.ConnectedMachineCount
		aggregatedHorizon.ProblematicMachineCount += pool.ProblematicMachineCount
		aggregatedHorizon.ActiveMachineCount += pool.ActiveMachineCount
		aggregatedHorizon.DeskPoolCount += 1
		if "true" == pool.Enabled {
			aggregatedHorizon.ActiveDeskPoolCount += 1
		} else {
			aggregatedHorizon.ProblematicDeskPoolCount += 1
		}

		// 组装单个桌面池的标签
		poolLabels := map[string]string{
			"desktopPoolId":            pool.ID,
			"desktopPoolName":          pool.Name,
			"enabled":                  pool.Enabled,
			"type":                     pool.Type,
			"authorizedUserCount":      strconv.Itoa(pool.AuthorizedUserCount),
			"sessionCount":             strconv.Itoa(pool.SessionCount),
			"activeSessionCount":       strconv.Itoa(pool.ActiveSessionCount),
			"connectedSessionCount":    strconv.Itoa(pool.ConnectedSessionCount),
			"disconnectedSessionCount": strconv.Itoa(pool.DisconnectedSessionCount),
			"machineCount":             strconv.Itoa(pool.MachineCount),
			"connectedMachineCount":    strconv.Itoa(pool.ConnectedMachineCount),
			"problematicMachineCount":  strconv.Itoa(pool.ProblematicMachineCount),
			"activeMachineCount":       strconv.Itoa(pool.ActiveMachineCount),
			"item_name":                "desktop_pool_info",
		}
		for k, v := range labels {
			poolLabels[k] = v
		}
		// 组装字段
		fields := map[string]interface{}{
			"desktop_pool_info": 1,
		}
		// 推送单个桌面池数据
		slist.PushSamples(inputName, fields, poolLabels)
	}

	// 组装聚合数据的标签
	aggregatedLabels := map[string]string{
		"authorizedUserCount":      strconv.Itoa(aggregatedHorizon.AuthorizedUserCount),
		"sessionCount":             strconv.Itoa(aggregatedHorizon.SessionCount),
		"activeSessionCount":       strconv.Itoa(aggregatedHorizon.ActiveSessionCount),
		"connectedSessionCount":    strconv.Itoa(aggregatedHorizon.ConnectedSessionCount),
		"disconnectedSessionCount": strconv.Itoa(aggregatedHorizon.DisconnectedSessionCount),

		"machineCount":            strconv.Itoa(aggregatedHorizon.MachineCount),
		"connectedMachineCount":   strconv.Itoa(aggregatedHorizon.ConnectedMachineCount),
		"problematicMachineCount": strconv.Itoa(aggregatedHorizon.ProblematicMachineCount),
		"activeMachineCount":      strconv.Itoa(aggregatedHorizon.ActiveMachineCount),

		"problematicDeskPoolCount": strconv.Itoa(aggregatedHorizon.ProblematicDeskPoolCount),
		"activeDeskPoolCount":      strconv.Itoa(aggregatedHorizon.ActiveDeskPoolCount),
		"deskPoolCount":            strconv.Itoa(aggregatedHorizon.DeskPoolCount),
		"item_name":                "horizon_info",
	}
	for k, v := range labels {
		aggregatedLabels[k] = v
	}
	// 组装字段
	fields := map[string]interface{}{
		"horizon_info": 1,
	}
	// 推送聚合数据
	slist.PushSamples(inputName, fields, aggregatedLabels)

}

// 获取桌面池授权信息
func getDesktopPoolEntitlements(ins *Instance, target, token string) (map[string]int, error) {
	// 发送请求获取数据
	entitlements, err := GetRequest(ins, target+desktopPoolsURL, token)
	if err != nil {
		return nil, err
	}

	// 结果存储
	result := make(map[string]int)

	// 遍历数据
	for _, entitlement := range entitlements {
		// 类型检查，防止 panic
		desktopPoolID, ok := entitlement["id"].(string)
		if !ok {
			log.Printf("E! Invalid id format in entitlement data: %v", entitlement)
			continue
		}
		adUserOrGroupIDs, ok := entitlement["ad_user_or_group_ids"].([]interface{})
		if !ok {
			log.Printf("E! Invalid ad_user_or_group_ids format in entitlement data: %v", entitlement)
			continue
		}
		// 记录授权用户数量
		result[desktopPoolID] = len(adUserOrGroupIDs)
	}

	return result, nil
}

// 获取桌面池基本信息
func getDesktopPoolInventory(ins *Instance, target, token string) (map[string]struct {
	Name    string
	Enabled string
	Type    string
}, error) {
	// 发送请求获取数据
	inventory, err := GetRequest(ins, target+inventoryDesktopPoolsURL, token)
	if err != nil {
		return nil, err
	}

	// 结果存储
	result := make(map[string]struct {
		Name    string
		Enabled string
		Type    string
	})

	// 遍历数据
	for _, item := range inventory {
		// 类型检查，防止 panic
		desktopPoolID, ok := item["id"].(string)
		if !ok {
			log.Printf("E! Invalid id format in inventory data: %v", item)
			continue
		}

		name, ok := item["name"].(string)
		if !ok {
			log.Printf("E! Invalid name format in inventory data: %v", item)
			continue
		}

		// 修改：enabled 字段是 bool 类型，需要转换为 string
		enabled, ok := item["enabled"].(bool)
		if !ok {
			log.Printf("E! Invalid enabled format in inventory data: %v", item)
			continue
		}
		enabledStr := strconv.FormatBool(enabled) // 将 bool 转换为 string

		poolType, ok := item["type"].(string)
		if !ok {
			log.Printf("E! Invalid type format in inventory data: %v", item)
			continue
		}

		// 记录桌面池基本信息
		result[desktopPoolID] = struct {
			Name    string
			Enabled string
			Type    string
		}{
			Name:    name,
			Enabled: enabledStr,
			Type:    poolType,
		}
	}

	return result, nil
}

// 获取桌面池会话信息
func getDesktopPoolSessions(ins *Instance, target, token string) (map[string]struct {
	SessionCount             int
	ActiveSessionCount       int
	ConnectedSessionCount    int
	DisconnectedSessionCount int
}, map[string]SessionData, error) {

	// 发起请求获取数据
	sessions, err := GetPaginatedRequest(ins, target+sessionsURL, token, 1000)
	if err != nil {
		return nil, nil, err
	}

	// 结果存储
	result := make(map[string]struct {
		SessionCount             int
		ActiveSessionCount       int
		ConnectedSessionCount    int
		DisconnectedSessionCount int
	})

	// 存储 会话数据
	sessionResult := make(map[string]SessionData)

	// 遍历获取的数据
	for _, session := range sessions {
		// 类型检查，防止 panic
		desktopPoolID, ok := session["desktop_pool_id"].(string)
		if !ok {
			log.Printf("E! Invalid desktop_pool_id format in session data: %v", session)
			continue
		}
		sessionState, ok := session["session_state"].(string)
		if !ok {
			log.Printf("E! Invalid session_state format in session data: %v", session)
			continue
		}
		// 获取现有的统计数据
		counts := result[desktopPoolID]
		counts.SessionCount++
		// 根据 session 状态更新对应的计数
		switch sessionState {
		case "CONNECTED":
			counts.ActiveSessionCount++
			counts.ConnectedSessionCount++
		case "DISCONNECTED":
			counts.DisconnectedSessionCount++
		}
		// 更新 map
		result[desktopPoolID] = counts

		// -------------------- 其他指标获取----------------------------
		sessionId, ok := session["id"].(string)
		if !ok {
			log.Printf("E! Invalid id format in session data: %v", session)
			continue
		}
		machineId, ok := session["machine_id"].(string)
		if !ok {
			log.Printf("E! Invalid machine_id format in session  data: %v", session)
			continue
		}
		startTime, ok := session["start_time"].(float64)
		if !ok {
			log.Printf("E! Invalid start_time format in session data: %v", session)
			continue
		}
		// 初始化 SessionData
		sessionData := SessionData{
			SessionId:     sessionId,
			SessionState:  sessionState,
			MachineId:     machineId,
			DesktopPoolId: desktopPoolID,
			StartTime:     int64(startTime),
		}
		if sessionDurationMs, ok := session["last_session_duration_ms"].(float64); ok {
			sessionData.SessionDurationMs = int64(sessionDurationMs)
		}
		if idleDuration, ok := session["idle_duration"].(float64); ok {
			sessionData.IdleDuration = int64(idleDuration)
		}
		if sessionProtocol, ok := session["session_protocol"].(string); ok {
			sessionData.SessionProtocol = sessionProtocol
		}
		if clientData, ok := session["client_data"].(map[string]interface{}); ok {
			if addr, ok := clientData["address"].(string); ok {
				sessionData.ClientIP = addr
			}
			if name, ok := clientData["name"].(string); ok {
				sessionData.ClientName = name
			}
		}
		// 退出时间
		if disconnectedTime, ok := session["disconnected_time"].(float64); ok {
			sessionData.LogonOutTimeMs = int64(disconnectedTime)
		}
		if userId, ok := session["user_id"].(string); ok {
			sessionData.UserId = userId
		}
		if brokerUserId, ok := session["broker_user_id"].(string); ok {
			sessionData.BrokerUserId = brokerUserId
		}
		// 存储会话数据
		sessionResult[sessionId] = sessionData

	}
	return result, sessionResult, nil
}

// 获取桌面池机器信息
func getDesktopPoolMachines(ins *Instance, target, token string) (map[string]MachineCountStats, map[string]MachineData, error) {
	// 发起请求获取数据
	machines, err := GetPaginatedRequest(ins, target+machinesURL, token, 1000)
	if err != nil {
		return nil, nil, err
	}
	// 结果存储
	result := make(map[string]MachineCountStats)
	// 存储 机器数据
	machineResult := make(map[string]MachineData)
	// 遍历获取的数据
	for _, machine := range machines {
		// 类型检查，防止 panic
		desktopPoolID, ok := machine["desktop_pool_id"].(string)
		if !ok {
			log.Printf("E! Invalid desktop_pool_id format in machine data: %v", machine)
			continue
		}
		vdiState, ok := machine["state"].(string)
		if !ok {
			log.Printf("E! Invalid state format in machine data: %v", machine)
			continue
		}
		// 获取现有的统计数据
		counts := result[desktopPoolID]
		counts.MachineCount++ //计算器总数
		switch vdiState {
		case "AVAILABLE":
			counts.ActiveMachineCount++
		case "CONNECTED":
			counts.ConnectedMachineCount++
		case "ERROR", "UNKNOWN": // 多个匹配条件
			counts.ProblematicMachineCount++
		default:
		}
		// 更新 map
		result[desktopPoolID] = counts

		// ---------- 其他指标获取 --------------------
		machineId, ok := machine["id"].(string)
		if !ok {
			log.Printf("E! Invalid machineId format in machine data: %v", machine)
			continue
		}
		machineName, ok := machine["name"].(string)
		if !ok {
			log.Printf("E! Invalid machineName format in machine data: %v", machine)
			continue
		}
		// 提取 用户名
		userName := extractUserName(machineName)
		managedData, ok := machine["managed_machine_data"].(map[string]interface{})
		if !ok {
			log.Printf("E! Invalid managed_machine_data format in machine data: %v", machine)
			continue
		}
		hostName, ok := managedData["host_name"].(string)
		if !ok {
			continue
		}
		// 提取配置服务器名称
		configuredServer := extractConfiguredServer(machine)
		// 存储 机器数据
		machineResult[machineId] = MachineData{
			MachineId:        machineId,
			MachineName:      machineName,
			UserName:         userName,
			HostName:         hostName,
			ConnectionServer: configuredServer,
		}
	}
	return result, machineResult, nil
}

// 从机器名称中提取用户名
func extractUserName(machineName string) string {
	if strings.HasPrefix(machineName, "VM") && len(machineName) > 4 {
		if strings.Contains(machineName, "-") {
			i := strings.Index(machineName, "-")
			if i > 4 {
				return machineName[4:i]
			}
		}
	}
	return ""
}

// 提取配置服务器名称
func extractConfiguredServer(machine map[string]interface{}) string {
	configuredServers, ok := machine["configured_by_connection_server"].([]interface{})
	if !ok || len(configuredServers) == 0 {
		return ""
	}
	configuredServer, ok := configuredServers[0].(string)
	if !ok {
		return ""
	}
	if strings.Contains(configuredServer, ".") {
		return strings.ToLower(strings.Split(configuredServer, ".")[0])
	}
	return strings.ToLower(configuredServer)
}
