package inputs

import (
	"encoding/json"
	"flashcat.cloud/categraf/types"
	"flashcat.cloud/categraf/types/metric"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"log"
	"strings"
	"time"
)

// KafkaConfig Kafka配置结构体
type KafkaConfig struct {
	Brokers      []string `toml:"brokers"`       // Kafka broker地址列表
	Topic        string   `toml:"topic"`         // Kafka topic名称
	RequiredAcks int      `toml:"required_acks"` // 确认模式: 0-无需确认,1-仅Leader确认,-1-全部副本确认
	Timeout      string   `toml:"timeout"`       // 超时时间字符串，如"10s"
}

// KafkaProducer Kafka生产者结构体
type KafkaProducer struct {
	Producer sarama.SyncProducer // sarama同步生产者
	Config   KafkaConfig         // Kafka配置
}

// NewKafkaProducer 创建新的Kafka生产者实例
func NewKafkaProducer(config KafkaConfig) (*KafkaProducer, error) {
	// 初始化Kafka配置
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.RequiredAcks = sarama.RequiredAcks(config.RequiredAcks)
	saramaConfig.Producer.Return.Successes = true // 启用成功交付通知

	// 解析超时时间
	timeout, err := time.ParseDuration(config.Timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timeout: %v", err)
	}
	saramaConfig.Producer.Timeout = timeout

	// 创建Kafka生产者
	producer, err := sarama.NewSyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %v", err)
	}

	return &KafkaProducer{
		Producer: producer,
		Config:   config,
	}, nil
}

// Influxdb2Kafka 将InfluxDB行协议数据转换为JSON并发送到Kafka
func (kp *KafkaProducer) Influxdb2Kafka(input []byte, currentTimestamp int64) error {
	metrics := make([]types.Metric, 0)
	decoder := lineprotocol.NewDecoderWithBytes(input)
	// 解析InfluxDB行协议数据
	for decoder.Next() {
		m, err := nextMetric(decoder)
		if err != nil {
			log.Printf("E! failed to parse influx line: %q, error: %v", string(input), err)
			continue
		}
		metrics = append(metrics, m)
	}
	// 处理每个指标数据
	for _, m := range metrics {
		// 构建要发送的数据结构
		data := map[string]interface{}{
			"eventName": m.Name(),         // 指标名称
			"fields":    m.Fields(),       // 指标字段
			"tags":      m.Tags(),         // 指标标签
			"ts":        currentTimestamp, // 时间戳
		}
		// 发送到Kafka
		if err := kp.SendToKafka(data); err != nil {
			return fmt.Errorf("failed to send data to Kafka: %v", err)
		}
	}
	return nil
}

// nextMetric 从解码器获取下一个指标
func nextMetric(decoder *lineprotocol.Decoder) (types.Metric, error) {
	measurement, err := decoder.Measurement()
	if err != nil {
		return nil, err
	}
	m := metric.New(string(measurement), nil, nil, time.Time{})
	// 处理标签
	for {
		key, value, err := decoder.NextTag()
		if err != nil {
			// 允许空标签
			if strings.Contains(err.Error(), "empty tag name") {
				break
			}
			return nil, err
		} else if key == nil {
			break
		}
		m.AddTag(string(key), string(value))
	}
	// 处理字段
	for {
		key, value, err := decoder.NextField()
		if err != nil {
			// 允许空字段
			if strings.Contains(err.Error(), "expected field key") {
				break
			}
			return nil, err
		} else if key == nil {
			break
		}
		m.AddField(string(key), value.Interface())
	}
	return m, nil
}

// SendMapToKafka 发送map数据到Kafka (优化后的统一发送方法)
func (kp *KafkaProducer) SendToKafka(data interface{}) error {
	// 将数据转换为JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %v", err)
	}
	// 构建Kafka消息
	msg := &sarama.ProducerMessage{
		Topic: kp.Config.Topic,
		Value: sarama.StringEncoder(jsonData),
	}
	// 发送消息
	_, _, err = kp.Producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message to Kafka: %v", err)
	}
	return nil
}

// SendMessage 发送带标签和映射的消息到Kafka (兼容旧方法)
func (kp *KafkaProducer) SendMessage(fields map[string]interface{}, mappingLabels map[string]string,
	labels map[string]string, currentTimestamp int64) error {

	// 合并所有数据
	data := make(map[string]interface{})
	for k, v := range fields {
		data[k] = v
	}
	for k, v := range mappingLabels {
		data[k] = v
	}
	for k, v := range labels {
		data[k] = v
	}
	data["apiClock"] = currentTimestamp // 添加时间戳

	return kp.SendToKafka(data) // 使用统一的发送方法
}

// Close 关闭Kafka生产者
func (kp *KafkaProducer) Close() error {
	if kp.Producer != nil {
		if err := kp.Producer.Close(); err != nil {
			return fmt.Errorf("failed to close Kafka producer: %v", err)
		}
	}
	return nil
}
