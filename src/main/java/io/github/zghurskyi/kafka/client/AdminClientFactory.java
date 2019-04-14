package io.github.zghurskyi.kafka.client;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;

public class AdminClientFactory {

    private static final String PROVISIONING_ADMIN_CLIENT_ID = "provisioning-admin-client";

    private final String brokers;

    private AdminClient adminClient;

    public AdminClientFactory(String brokers) {
        this.brokers = brokers;
    }

    public synchronized AdminClient getAdminClient() {
        if (this.adminClient == null) {
            Properties configs = getConfigs(brokers);
            this.adminClient = AdminClient.create(configs);
        }
        return this.adminClient;
    }

    private Properties getConfigs(String brokers) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, PROVISIONING_ADMIN_CLIENT_ID);
        return config;
    }
}
