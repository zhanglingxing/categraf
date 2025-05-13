# 应用场景
获取horizon相关指标

# 指标说明
```
获取horizon相关的业务指标
- horizon_datastore_local
- horizon_desktop_pool_info
- horizon_horizon_info
- horizon_connection_server_info
- horizon_user_session_info
监控插件执行耗时指标
- horizon_exec_duration_time
```

# 修改horizon.toml文件配置

``` 以下文件内容配置作为参考
[root@aliyun input.horizon]# cat horizon.toml
interval = 60

## Set the mapping of extra tags in batches
[mappings]
 "https://172.24.100.21" = { "vcenter" = "172.24.98.199","horizonName" = "xamp-cs.wxmp" }

[[instances]]
targets = [
    "https://172.24.100.21"
]

## append some labels for series
#labels = { "vcenter" = "vc00.vdi.sh.moonpac.com"  }

## interval = global.interval * interval_times
# interval_times = 1

## Set response_timeout (default 5 seconds)
response_timeout = "30s"

## Optional HTTP Basic Auth Credentials
username = "csuser01"
password = "Iop[]=-09*"
domain = "wxmp.com"

## Kafka configuration
[kafka]
# Kafka broker addresses
brokers = ["kafka1:9092", "kafka2:9092"]  
# Kafka topic to send data to
topic = "horizon-metrics"                 
# Number of acknowledgments required (0, 1, -1)
required_acks = 1                         
timeout = "10s"
```

# 业务指标样例数据
```agsl
1741765132 15:38:52 horizon_datastore_local agent_hostname=10.173.21.202 datastoreLocal=true datastoreName=iso horizonName=Cluster-HORIZON item_name=datastore_local target=https://172.24.100.21 vcenter=vc00.vdi.sh.moonpac.com 1
1741765132 15:38:52 horizon_datastore_local agent_hostname=10.173.21.202 datastoreLocal=true datastoreName=local-store-172.24.98.253-data horizonName=Cluster-HORIZON item_name=datastore_local target=https://172.24.100.21 vcenter=vc00.vdi.sh.moonpac.com 1
1741765132 15:38:52 horizon_datastore_local agent_hostname=10.173.21.202 datastoreLocal=true datastoreName=local-store-172.24.98.253-sys horizonName=Cluster-HORIZON item_name=datastore_local target=https://172.24.100.21 vcenter=vc00.vdi.sh.moonpac.com 1
1741765132 15:38:52 horizon_datastore_local agent_hostname=10.173.21.202 datastoreLocal=true datastoreName=vSphere horizonName=Cluster-HORIZON item_name=datastore_local target=https://172.24.100.21 vcenter=vc00.vdi.sh.moonpac.com 1
1741765132 15:38:52 horizon_desktop_pool_info activeSessionCount=0 agent_hostname=10.173.21.202 authorizedUserCount=22 connectedMachineCount=0 connectedSessionCount=0 desktopPoolId=79c790c6-67a2-4a05-92fe-a7613bfd1b06 desktopPoolName=xamp disconnectedSessionCount=0 enabled=true horizonName=Cluster-HORIZON item_name=desktop_pool_info machineCount=2 problematicMachineCount=2 sessionCount=0 target=https://172.24.100.21 type=MANUAL vcenter=vc00.vdi.sh.moonpac.com 1
1741765132 15:38:52 horizon_desktop_pool_info activeSessionCount=0 agent_hostname=10.173.21.202 authorizedUserCount=7 connectedMachineCount=0 connectedSessionCount=0 desktopPoolId=72c1b8d4-7d02-46e0-9f71-015c2d2a3557 desktopPoolName=test2 disconnectedSessionCount=0 enabled=true horizonName=Cluster-HORIZON item_name=desktop_pool_info machineCount=0 problematicMachineCount=0 sessionCount=0 target=https://172.24.100.21 type=MANUAL vcenter=vc00.vdi.sh.moonpac.com 1
1741765132 15:38:52 horizon_desktop_pool_info activeSessionCount=0 agent_hostname=10.173.21.202 authorizedUserCount=3 connectedMachineCount=1 connectedSessionCount=0 desktopPoolId=c11443a0-d313-447b-8b37-1469dce6deae desktopPoolName=U22113 disconnectedSessionCount=0 enabled=true horizonName=Cluster-HORIZON item_name=desktop_pool_info machineCount=1 problematicMachineCount=0 sessionCount=0 target=https://172.24.100.21 type=MANUAL vcenter=vc00.vdi.sh.moonpac.com 1
1741765132 15:38:52 horizon_desktop_pool_info activeSessionCount=1 agent_hostname=10.173.21.202 authorizedUserCount=2 connectedMachineCount=6 connectedSessionCount=1 desktopPoolId=221fc64f-0075-4883-ad54-4cf1f30b09ab desktopPoolName=test1 disconnectedSessionCount=0 enabled=true horizonName=Cluster-HORIZON item_name=desktop_pool_info machineCount=6 problematicMachineCount=0 sessionCount=1 target=https://172.24.100.21 type=MANUAL vcenter=vc00.vdi.sh.moonpac.com 1
1741765132 15:38:52 horizon_horizon_info activeDeskPoolCount=4 activeSessionCount=1 agent_hostname=10.173.21.202 authorizedUserCount=34 connectedMachineCount=7 connectedSessionCount=1 deskPoolCount=4 disconnectedSessionCount=0 horizonName=Cluster-HORIZON item_name=horizon_info machineCount=9 problematicDeskPoolCount=0 problematicMachineCount=2 sessionCount=1 target=https://172.24.100.21 vcenter=vc00.vdi.sh.moonpac.com 1
1741765132 15:38:52 horizon_connection_server_info agent_hostname=10.173.21.202 csName=XAMP-CS horizonName=Cluster-HORIZON item_name=connection_server_info status=OK target=https://172.24.100.21 valid=false validFrom=1694608324000 validTo=1765888324000 vcenter=vc00.vdi.sh.moonpac.com version=8.2.0 1
```

# 插件耗时监控数据
```agsl
1741765132 15:38:52 horizon_exec_duration_time agent_hostname=10.173.21.202 horizonName=Cluster-HORIZON target=https://172.24.100.21 vcenter=vc00.vdi.sh.moonpac.com 1.369887106
```

# Horizon API 数据指标说明

## 通用字段
- **timestamp**: 记录数据的时间戳（Unix时间）。
- **agent_hostname**: 记录数据的代理服务器地址。
- **horizonName**: Horizon环境的名称。
- **target**: Horizon API的目标URL。
- **vcenter**: 关联的vCenter服务器地址。

## 数据存储 (horizon_datastore_local)
- **datastoreLocal**: 是否为本地数据存储。
- **datastoreName**: 数据存储的名称。
- **item_name**: 记录类型，表示数据存储信息。

## 桌面池信息 (horizon_desktop_pool_info)
- **desktopPoolId**: 桌面池的唯一标识。
- **desktopPoolName**: 桌面池的名称。
- **activeSessionCount**: 当前活跃的会话数量。
- **authorizedUserCount**: 被授权访问该桌面池的用户数量。
- **connectedMachineCount**: 当前处于连接状态的虚拟机数量。
- **connectedSessionCount**: 当前已建立连接的会话数量。
- **disconnectedSessionCount**: 断开的会话数量。
- **enabled**: 该桌面池是否启用。
- **machineCount**: 桌面池中的虚拟机总数。
- **problematicMachineCount**: 发生故障的虚拟机数量。
- **sessionCount**: 该桌面池中的所有会话总数。
- **type**: 桌面池的类型。
- **item_name**: 记录类型，表示桌面池信息。

## Horizon 环境信息 (horizon_horizon_info)
- **activeDeskPoolCount**: 活跃桌面池的数量。
- **activeSessionCount**: 活跃会话的数量。
- **authorizedUserCount**: 被授权访问Horizon环境的用户数量。
- **connectedMachineCount**: 处于连接状态的虚拟机数量。
- **connectedSessionCount**: 当前已建立连接的会话数量。
- **deskPoolCount**: Horizon环境中的桌面池总数。
- **disconnectedSessionCount**: 断开的用户会话数量。
- **machineCount**: 该环境中的虚拟机总数。
- **problematicDeskPoolCount**: 发生故障的桌面池数量。
- **problematicMachineCount**: 发生故障的机器数量。
- **sessionCount**: 该Horizon环境中的所有会话数量。
- **item_name**: 记录类型，表示Horizon环境信息。

## 连接服务器信息 (horizon_connection_server_info)
- **csName**: 连接服务器的名称。
- **status**: 服务器的当前状态。
- **valid**: 证书或授权是否有效。
- **validFrom**: 证书的生效时间（Unix时间戳）。
- **validTo**: 证书的到期时间（Unix时间戳）。
- **version**: 连接服务器的Horizon版本。
- **item_name**: 记录类型，表示连接服务器信息。


## 总结
本数据反映了Horizon环境的存储、桌面池、整体运行状况以及连接服务器状态，可用于监控虚拟桌面基础设施（VDI）的健康状况。
