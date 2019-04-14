package io.github.zghurskyi.kafka.client;

import io.github.zghurskyi.kafka.TopicProvisionException;
import io.github.zghurskyi.kafka.printer.JsonPrinter;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public final class AdminClientOperations {

    private static final Logger log = LoggerFactory.getLogger(AdminClientOperations.class);

    private static final int PROVISIONING_TIMEOUT_SEC = 60;

    private AdminClientOperations() {
        throw new UnsupportedOperationException("Instantiation is not supported!");
    }

    public static void createTopics(AdminClient client, Collection<NewTopic> topics) {
        apply(() -> {
            if (topics.isEmpty()) {
                return null;
            }
            log.info("About to create topics: {}", JsonPrinter.print(topics));
            CreateTopicsResult createTopicsResult = client.createTopics(topics);
            createTopicsResult.all().get(PROVISIONING_TIMEOUT_SEC, TimeUnit.SECONDS);
            return null;
        });
    }

    public static void createPartitions(AdminClient client, Map<String, NewPartitions> partitions) {
        apply(() -> {
            if (partitions.isEmpty()) {
                return null;
            }
            CreatePartitionsResult createPartitionsResult = client.createPartitions(partitions);
            createPartitionsResult.all().get(PROVISIONING_TIMEOUT_SEC, TimeUnit.SECONDS);
            return null;
        });
    }

    public static void alterConfigs(AdminClient client, Map<ConfigResource, Config> configs) {
        apply(() -> {
            if (configs.isEmpty()) {
                return null;
            }
            AlterConfigsResult alterConfigsResult = client.alterConfigs(configs);
            alterConfigsResult.all().get(PROVISIONING_TIMEOUT_SEC, TimeUnit.SECONDS);
            return null;
        });
    }

    @SuppressWarnings("unchecked")
    public static Set<String> listTopics(AdminClient client) {
        return (Set<String>) apply(() -> {
            ListTopicsResult listTopicsResult = client.listTopics();
            return listTopicsResult.names().get(PROVISIONING_TIMEOUT_SEC, TimeUnit.SECONDS);
        });
    }

    @SuppressWarnings("unchecked")
    public static Map<String, TopicDescription> describeTopics(AdminClient client, Collection<String> topics) {
        return (Map<String, TopicDescription>) apply(() -> {
            if (topics.isEmpty()) {
                return Collections.emptyMap();
            }
            DescribeTopicsResult describeTopicsResult = client.describeTopics(topics);
            KafkaFuture<Map<String, TopicDescription>> topicDescriptionsFuture = describeTopicsResult.all();
            return topicDescriptionsFuture.get(PROVISIONING_TIMEOUT_SEC, TimeUnit.SECONDS);
        });
    }

    @SuppressWarnings("unchecked")
    public static Map<ConfigResource, Config> describeConfigs(AdminClient client, Set<String> topics) {
        return (Map<ConfigResource, Config>) apply(() -> {
            if (topics.isEmpty()) {
                return Collections.emptyMap();
            }
            Set<ConfigResource> configResources = getConfigResources(topics);
            DescribeConfigsResult describeConfigsResult = client.describeConfigs(configResources);
            Map<ConfigResource, Config> currentConfig = describeConfigsResult.all().get(PROVISIONING_TIMEOUT_SEC, TimeUnit.SECONDS);
            log.debug("Current config on the broker: {}", JsonPrinter.print(currentConfig));
            return currentConfig;
        });
    }

    private static Set<ConfigResource> getConfigResources(Set<String> topicNames) {
        return topicNames.stream()
            .map(topicName -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
            .collect(Collectors.toSet());
    }

    private interface AdminOperation {
        Object run() throws InterruptedException, ExecutionException, TimeoutException;
    }

    private static Object apply(AdminOperation operation) {
        try {
            return operation.run();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            String message = "Kafka provision thread was interrupted! Error: " + exception.getMessage();
            log.error(message, exception);
            throw new TopicProvisionException(message, exception);
        } catch (ExecutionException | TimeoutException exception) {
            String message = "Failed to provision topics! Error: " + exception.getMessage();
            log.error(message, exception);
            throw new TopicProvisionException(message, exception);
        }
    }
}
