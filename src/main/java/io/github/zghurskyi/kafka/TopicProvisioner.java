package io.github.zghurskyi.kafka;

import io.github.zghurskyi.kafka.client.AdminClientFactory;
import io.github.zghurskyi.kafka.client.AdminClientOperations;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryOperations;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TopicProvisioner {

    private static final Logger log = LoggerFactory.getLogger(TopicProvisioner.class);

    private final ProvisionProperties provisionProperties;
    private final AdminClientFactory adminClientFactory;
    private final RetryOperations retryOperations;

    @java.beans.ConstructorProperties({"provisionProperties", "adminClientFactory", "retryOperations"})
    TopicProvisioner(ProvisionProperties provisionProperties, AdminClientFactory adminClientFactory, RetryOperations retryOperations) {
        this.provisionProperties = provisionProperties;
        this.adminClientFactory = adminClientFactory;
        this.retryOperations = retryOperations;
    }

    @PostConstruct
    public void provisionTopics() {
        try (AdminClient adminClient = adminClientFactory.getAdminClient()) {
            retryOperations.<Void, Exception>execute(context -> {
                Set<String> topicsOnBroker = AdminClientOperations.listTopics(adminClient);
                log.debug("Found following topics on the broker: {}", topicsOnBroker);

                if (provisionProperties.isAutoCreateTopics()) {
                    Set<NewTopic> newTopics = getNewTopics(topicsOnBroker);
                    AdminClientOperations.createTopics(adminClient, newTopics);
                }

                if (provisionProperties.isAutoAddPartitions()) {
                    Map<String, NewPartitions> newPartitions = getNewPartitions(adminClient, topicsOnBroker);
                    AdminClientOperations.createPartitions(adminClient, newPartitions);
                }

                if (provisionProperties.isAutoUpdateConfig()) {
                    Map<ConfigResource, Config> updatedConfig = getUpdatedConfigs(adminClient, topicsOnBroker);
                    AdminClientOperations.alterConfigs(adminClient, updatedConfig);
                }

                return null;
            });
        } catch (Exception exception) {
            String message = "Failed to provision topics! Error: " + exception.getMessage();
            log.error(message, exception);
            throw new TopicProvisionException(message, exception);
        }
    }

    private Set<NewTopic> getNewTopics(Set<String> topics) {
        return provisionProperties.getTopics().stream()
            .filter(topicConfig -> !topics.contains(topicConfig.getName()))
            .map(topicConfig -> newTopic(topicConfig).configs(topicConfig.getConfigs()))
            .collect(Collectors.toSet());
    }

    private Map<String, NewPartitions> getNewPartitions(AdminClient adminClient, Set<String> topicsOnBroker) {
        Map<String, TopicDescription> topicDescriptions =
            AdminClientOperations.describeTopics(adminClient, getTopicsToScale(topicsOnBroker));
        return getNewPartitionsByTopic(topicDescriptions);
    }

    private Map<ConfigResource, Config> getUpdatedConfigs(AdminClient adminClient, Set<String> topicsOnBroker) {
        Map<ConfigResource, Config> currentConfig = AdminClientOperations.describeConfigs(adminClient, topicsOnBroker);
        return getUpdatedConfig(currentConfig);
    }

    private NewTopic newTopic(ProvisionProperties.TopicProperties topicConfig) {
        return new NewTopic(topicConfig.getName(), topicConfig.getNumPartitions(), topicConfig.getReplicationFactor());
    }

    private Set<String> getTopicsToScale(Set<String> existingTopics) {
        return provisionProperties.getTopics().stream()
            .map(ProvisionProperties.TopicProperties::getName)
            .filter(existingTopics::contains)
            .collect(Collectors.toSet());
    }

    private Map<String, NewPartitions> getNewPartitionsByTopic(Map<String, TopicDescription> descriptions) {
        Map<String, NewPartitions> topicsToScale = new HashMap<>();
        descriptions.forEach((topicName, topicDescription) -> {
            int actualPartitionNumber = topicDescription.partitions().size();
            int configPartitionNumber = getNumPartitions(topicName);
            if (actualPartitionNumber < configPartitionNumber) {
                topicsToScale.put(topicName, NewPartitions.increaseTo(configPartitionNumber));
            }
            logPartitionsUpdate(topicName, actualPartitionNumber, configPartitionNumber);
        });
        return topicsToScale;
    }

    private Map<ConfigResource, Config> getUpdatedConfig(Map<ConfigResource, Config> currentConfig) {
        return currentConfig.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                config -> new Config(getUpdatedConfigEntries(config.getKey(), config.getValue()))));
    }

    private int getNumPartitions(String topicName) {
        return provisionProperties.getTopics().stream()
            .filter(topic -> topic.getName().equals(topicName))
            .map(ProvisionProperties.TopicProperties::getNumPartitions)
            .findAny()
            .orElse(1);
    }

    private void logPartitionsUpdate(String topicName, int actualPartitionNumber, int configPartitionNumber) {
        if (actualPartitionNumber < configPartitionNumber) {
            log.debug("About to add partitions '{}': {}", topicName, configPartitionNumber - actualPartitionNumber);
        } else if (actualPartitionNumber > configPartitionNumber) {
            log.warn("Configured partition number is less then actual. Topic '{}': config {}, actual {}.",
                topicName, configPartitionNumber, actualPartitionNumber);
        }
    }

    private Set<ConfigEntry> getUpdatedConfigEntries(ConfigResource configResource, Config currentConfigMap) {
        return provisionProperties.getTopics().stream()
            .filter(topic -> topic.getName().equals(configResource.name()))
            .map(ProvisionProperties.TopicProperties::getConfigs)
            .flatMap(map -> map.entrySet().stream())
            .map(configEntry -> new ConfigEntry(configEntry.getKey(), configEntry.getValue()))
            .peek(configEntry -> logConfigUpdate(configResource, currentConfigMap, configEntry))
            .collect(Collectors.toSet());
    }

    private void logConfigUpdate(ConfigResource configResource, Config currentConfigMap, ConfigEntry updatedConfigEntry) {
        ConfigEntry currentConfig = currentConfigMap.get(updatedConfigEntry.name());
        if (currentConfig != null && !currentConfig.value().equals(updatedConfigEntry.value())) {
            log.warn("Topic '{}': config '{}' changed from {} to {}", configResource.name(),
                updatedConfigEntry.name(), currentConfig.value(), updatedConfigEntry.value());
        } else {
            log.debug("Topic '{}': new config added '{}' = {}", configResource.name(),
                updatedConfigEntry.name(), updatedConfigEntry.value());
        }
    }
}