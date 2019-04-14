package io.github.zghurskyi.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Properties;

final class TestAdminClientUtil {

    private static final String TEST_ADMIN_CLIENT_ID = "test-admin-client";

    private TestAdminClientUtil() {
        throw new UnsupportedOperationException("Instantiation is not supported!");
    }

    static AdminClient createAdminClient(String brokers) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, TEST_ADMIN_CLIENT_ID);
        return AdminClient.create(config);
    }

    static int getPartitionsCount(AdminClient adminClient, String topicName) throws Exception {
        return adminClient.describeTopics(Collections.singleton(topicName))
            .all()
            .get()
            .get(topicName)
            .partitions()
            .size();
    }

    static Config getTopicConfig(AdminClient adminClient, String topicName) throws Exception {
        ConfigResource topic = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        return adminClient.describeConfigs(Collections.singleton(topic))
            .all()
            .get()
            .get(topic);
    }
}