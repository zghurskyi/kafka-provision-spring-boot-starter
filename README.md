# Kafka Provision Spring Boot Starter

[![Build Status](https://travis-ci.org/zghurskyi/kafka-provision-spring-boot-starter.svg?branch=master)](https://travis-ci.org/zghurskyi/kafka-provision-spring-boot-starter)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=zghurskyi_kafka-provisioner-spring-boot-starter&metric=alert_status)](https://sonarcloud.io/dashboard?id=zghurskyi_kafka-provisioner-spring-boot-starter)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=zghurskyi_kafka-provisioner-spring-boot-starter&metric=coverage)](https://sonarcloud.io/dashboard?id=zghurskyi_kafka-provisioner-spring-boot-starter)

This repository contains Spring Boot Starter, that enables distributed Kafka topics provisioning and centralized topic configs management at the same time.

Following featurs are supported:
- creating new topics
- adding partitions to the existing topics
- updating [topic configurations](https://kafka.apache.org/documentation/#topicconfigs)

# Getting Started

To enable automatic topic creation and configuration follow the steps:

1. Add dependency

```groovy
ext {
    kafkaProvisionVersion = "1.0.0"
}
 
dependencies {
    implementation "io.github.zghurskyi.kafka:kafka-provision-spring-boot-starter:${kafkaProvisionVersion}"
}
```

2. Add `@EnableTopicProvisioning`

```java
@SpringBootApplication
@EnableTopicProvisioning
public class Application {
 
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
```

3. Configure topics in `application.yml`

```yaml
# disable topic creation by Spring Cloud Stream
spring.cloud.stream.kafka.binder:
  auto-create-topics: false

# configure topic provisioning
kafka.provision:
  brokers: ${kafka.brokers}
  topics:
  - name: ${topic.one}
    numPartitions: ${topic.one.partitions}
    replicationFactor: ${topic.one.replication.factor}
    configs:
      cleanup.policy: ${topic.one.cleanup.policy}
      retention.ms: ${topic.one.retention.ms}
  - name: ${topic.two}
    numPartitions: ${topic.two.partitions}
    replicationFactor: ${topic.two.replication.factor}
    configs:
      cleanup.policy: ${topic.two.cleanup.policy}
      retention.ms: ${topic.two.retention.ms}
```
> Note: Any valid [Kafka topic config](https://kafka.apache.org/documentation/#topicconfigs) can be used in `configs` section.

4. (Optional) Disable provisioning during tests in `application-test.yml`:

```yaml
kafka.provision.enabled: false
```

# Externalized configuration

The starter allows centralized management of topic configurations and decentralized topic provisioning at the same time. 

This is possible, since provisioning is specified through application properties. So, simply manage Kafka topics state in your system through environment variables, that are collected together in centralized place:

- `Dockerfile`:

```dockerfile
ENV KAFKA_BROKERS kafka-broker-0:9092,kafka-broker-1:9092
ENV TOPIC_ONE topic_one
ENV TOPIC_ONE_PARTITIONS 32
ENV TOPIC_ONE_REPLICATION_FACTOR 3
ENV TOPIC_ONE_CLEANUP_POLICY delete
ENV TOPIC_ONE_RETENTION_MS 7776000000
ENV TOPIC_TWO topic_one
ENV TOPIC_TWO_PARTITIONS 32
ENV TOPIC_TWO_REPLICATION_FACTOR 3
ENV TOPIC_TWO_CLEANUP_POLICY delete
ENV TOPIC_TWO_RETENTION_MS 7776000000
```
- Docker Swarm config

- Kubernetes `configmap.yaml`:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  creationTimestamp: 2019-04-14T18:38:05Z
  name: kafka-topics-config
  namespace: default
  selfLink: /api/v1/namespaces/default/configmaps/kafka-topics-config
data:
  KAFKA_BROKERS: kafka-broker-0:9092,kafka-broker-1:9092
  TOPIC_ONE: topic_one
  TOPIC_ONE_PARTITIONS: 32
  TOPIC_ONE_REPLICATION_FACTOR: 3
  TOPIC_ONE_CLEANUP_POLICY: delete
  TOPIC_ONE_RETENTION_MS: 7776000000
  TOPIC_TWO: topic_one
  TOPIC_TWO_PARTITIONS: 32
  TOPIC_TWO_REPLICATION_FACTOR: 3
  TOPIC_TWO_CLEANUP_POLICY: delete
  TOPIC_TWO_RETENTION_MS: 7776000000
```
