package io.github.zghurskyi.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.rule.OutputCapture;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.containsString;

@DirtiesContext
public class TopicProvisionAutoConfigurationTest {

    private static final String EXISTING_TOPIC = "existing-topic";

    @ClassRule
    public static final KafkaEmbedded EMBEDDED_KAFKA = new KafkaEmbedded(1, true, EXISTING_TOPIC);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public OutputCapture output = new OutputCapture();

    private ConfigurableApplicationContext context;

    @Test
    public void kafkaTopicProvisionerIsNotCreatedIfBrokersPropertyIsMissing() {
        this.context = TestContextLoader.load(EmptyConfiguration.class,
            "kafka.provision.topics[0].name: existing-topic",
            "kafka.provision.topics[0].numPartitions: 8",
            "kafka.provision.topics[0].replicationFactor: 1");
        this.thrown.expect(NoSuchBeanDefinitionException.class);
        this.thrown.expectMessage("No qualifying bean of type 'io.github.zghurskyi.kafka.TopicProvisioner' available");

        this.context.getBean(TopicProvisioner.class);
    }

    @Test
    public void kafkaTopicProvisionerIsNotCreatedIfDisabled() {
        this.context = TestContextLoader.load(EmptyConfiguration.class,
            "kafka.provision.enabled: false",
            "kafka.provision.brokers: ${spring.embedded.kafka.brokers}",
            "kafka.provision.topics[0].name: existing-topic",
            "kafka.provision.topics[0].numPartitions: 8",
            "kafka.provision.topics[0].replicationFactor: 1");
        this.thrown.expect(NoSuchBeanDefinitionException.class);
        this.thrown.expectMessage("No qualifying bean of type 'io.github.zghurskyi.kafka.TopicProvisioner' available");

        this.context.getBean(TopicProvisioner.class);
    }

    @Test
    public void topicIsNotCreatedIfTopicNameLengthIsInvalid() throws Exception {
        this.thrown.expect(UnsatisfiedDependencyException.class);
        this.thrown.expectMessage(containsString("Failed to bind properties under 'kafka.provision'"));

        this.context = TestContextLoader.load(EmptyConfiguration.class,
            "kafka.provision.brokers: ${spring.embedded.kafka.brokers}",
            "kafka.provision.topics[0].name: " + StringUtils.repeat("a", 250),
            "kafka.provision.topics[0].numPartitions: 4",
            "kafka.provision.topics[0].replicationFactor: 1");
    }

    @Test
    public void topicIsNotCreatedIfTopicNameIsInvalid() throws Exception {
        this.thrown.expect(UnsatisfiedDependencyException.class);
        this.thrown.expectMessage(containsString("Failed to bind properties under 'kafka.provision'"));

        this.context = TestContextLoader.load(EmptyConfiguration.class,
            "kafka.provision.brokers: ${spring.embedded.kafka.brokers}",
            "kafka.provision.topics[0].name: invalid#",
            "kafka.provision.topics[0].numPartitions: 4",
            "kafka.provision.topics[0].replicationFactor: 1");
    }

    private void assertLogMessage(String text) {
        this.output.expect(containsString(text));
    }

    @Test
    public void topicIsNotCreatedIfAlreadyExists() throws Exception {
        this.context = TestContextLoader.load(EmptyConfiguration.class,
            "kafka.provision.brokers: ${spring.embedded.kafka.brokers}",
            "kafka.provision.topics[0].name: existing-topic",
            "kafka.provision.topics[0].numPartitions: 7",
            "kafka.provision.topics[0].replicationFactor: 1");

        AdminClient adminClient = TestAdminClientUtil.createAdminClient(EMBEDDED_KAFKA.getBrokersAsString());

        this.context.getBean(TopicProvisioner.class);

        assertThat(adminClient.listTopics().names().get()).contains(EXISTING_TOPIC);
    }

    @Test
    public void topicPartitionsAreAddedIfTopicAlreadyExists() throws Exception {
        this.context = TestContextLoader.load(EmptyConfiguration.class,
            "kafka.provision.brokers: ${spring.embedded.kafka.brokers}",
            "kafka.provision.topics[0].name: existing-topic",
            "kafka.provision.topics[0].numPartitions: 8",
            "kafka.provision.topics[0].replicationFactor: 1");

        AdminClient adminClient = TestAdminClientUtil.createAdminClient(EMBEDDED_KAFKA.getBrokersAsString());

        this.context.getBean(TopicProvisioner.class);

        assertThat(TestAdminClientUtil.getPartitionsCount(adminClient, EXISTING_TOPIC)).isEqualTo(8);
    }

    @Test
    public void topicPartitionsAreNotAddedIfAutoAddPartitionsIsDisabled() throws Exception {
        this.context = TestContextLoader.load(EmptyConfiguration.class,
            "kafka.provision.brokers: ${spring.embedded.kafka.brokers}",
            "kafka.provision.autoAddPartitions: false",
            "kafka.provision.topics[0].name: existing-topic",
            "kafka.provision.topics[0].numPartitions: 9",
            "kafka.provision.topics[0].replicationFactor: 1");

        AdminClient adminClient = TestAdminClientUtil.createAdminClient(EMBEDDED_KAFKA.getBrokersAsString());
        int partitionsCountBefore = TestAdminClientUtil.getPartitionsCount(adminClient, EXISTING_TOPIC);

        this.context.getBean(TopicProvisioner.class);

        int partitionsCountAfter = TestAdminClientUtil.getPartitionsCount(adminClient, EXISTING_TOPIC);

        assertThat(partitionsCountBefore).isEqualTo(partitionsCountAfter);
    }

    @Test
    public void singleTopicIsCreatedAndConfigured() throws Exception {
        this.context = TestContextLoader.load(EmptyConfiguration.class,
            "kafka.provision.brokers: ${spring.embedded.kafka.brokers}",
            "kafka.provision.topics[0].name: test",
            "kafka.provision.topics[0].numPartitions: 8",
            "kafka.provision.topics[0].replicationFactor: 1");

        AdminClient adminClient = TestAdminClientUtil.createAdminClient(EMBEDDED_KAFKA.getBrokersAsString());

        this.context.getBean(TopicProvisioner.class);

        assertThat(adminClient.listTopics().names().get()).contains("test");
        assertLogMessage(
            "About to create topics: [ {\n"
                + "  \"name\" : \"test\",\n"
                + "  \"numPartitions\" : 8,\n"
                + "  \"replicationFactor\" : 1,\n"
                + "  \"replicasAssignments\" : null,\n"
                + "  \"configs\" : { }\n"
                + "} ]");
    }

    @Test
    public void topicConfigurationIsUpdated() throws Exception {
        this.context = TestContextLoader.load(EmptyConfiguration.class,
            "kafka.provision.brokers: ${spring.embedded.kafka.brokers}",
            "kafka.provision.topics[0].name: to_update",
            "kafka.provision.topics[0].numPartitions: 8",
            "kafka.provision.topics[0].replicationFactor: 1",
            "kafka.provision.topics[0].configs.cleanup.policy: delete");

        AdminClient adminClient = TestAdminClientUtil.createAdminClient(EMBEDDED_KAFKA.getBrokersAsString());

        this.context.getBean(TopicProvisioner.class);
        Config initialConfig = TestAdminClientUtil.getTopicConfig(adminClient, "to_update");
        assertThat(initialConfig.get("cleanup.policy").value()).isEqualTo("delete");

        this.context.close();
        this.context = TestContextLoader.load(EmptyConfiguration.class,
            "kafka.provision.brokers: ${spring.embedded.kafka.brokers}",
            "kafka.provision.topics[0].name: to_update",
            "kafka.provision.topics[0].numPartitions: 8",
            "kafka.provision.topics[0].replicationFactor: 1",
            "kafka.provision.topics[0].configs.cleanup.policy: compact");

        Config updatedConfig = TestAdminClientUtil.getTopicConfig(adminClient, "to_update");
        assertThat(updatedConfig.get("cleanup.policy").value()).isEqualTo("compact");
    }

    @Test
    public void topicConfigurationIsNotUpdatedIfAutoUpdateDisabled() throws Exception {
        this.context = TestContextLoader.load(EmptyConfiguration.class,
            "kafka.provision.brokers: ${spring.embedded.kafka.brokers}",
            "kafka.provision.topics[0].name: to_update1",
            "kafka.provision.topics[0].numPartitions: 8",
            "kafka.provision.topics[0].replicationFactor: 1",
            "kafka.provision.topics[0].configs.cleanup.policy: delete");

        AdminClient adminClient = TestAdminClientUtil.createAdminClient(EMBEDDED_KAFKA.getBrokersAsString());

        this.context.getBean(TopicProvisioner.class);
        Config initialConfig = TestAdminClientUtil.getTopicConfig(adminClient, "to_update1");
        assertThat(initialConfig.get("cleanup.policy").value()).isEqualTo("delete");

        this.context.close();
        this.context = TestContextLoader.load(EmptyConfiguration.class,
            "kafka.provision.brokers: ${spring.embedded.kafka.brokers}",
            "kafka.provision.autoUpdateConfig: false",
            "kafka.provision.topics[0].name: to_update1",
            "kafka.provision.topics[0].numPartitions: 8",
            "kafka.provision.topics[0].replicationFactor: 1",
            "kafka.provision.topics[0].configs.cleanup.policy: compact");

        this.context.getBean(TopicProvisioner.class);

        Config updatedConfig = TestAdminClientUtil.getTopicConfig(adminClient, "to_update1");
        assertThat(updatedConfig.get("cleanup.policy").value()).isEqualTo("delete");
    }

    @Test
    public void multipleTopicsAreCreatedAndConfigured() throws Exception {
        this.context = TestContextLoader.load(EmptyConfiguration.class,
            "kafka.provision.brokers: ${spring.embedded.kafka.brokers}",
            "kafka.provision.topics[0].name: test1",
            "kafka.provision.topics[0].numPartitions: 8",
            "kafka.provision.topics[0].replicationFactor: 1",
            "kafka.provision.topics[0].configs.cleanup.policy: delete",
            "kafka.provision.topics[0].configs.retention.ms: 1000",
            "kafka.provision.topics[1].name: test2",
            "kafka.provision.topics[1].numPartitions: 4",
            "kafka.provision.topics[1].replicationFactor: 1",
            "kafka.provision.topics[1].configs.cleanup.policy: delete",
            "kafka.provision.topics[1].configs.retention.ms: 2000");

        AdminClient adminClient = TestAdminClientUtil.createAdminClient(EMBEDDED_KAFKA.getBrokersAsString());

        this.context.getBean(TopicProvisioner.class);

        assertThat(adminClient.listTopics().names().get()).contains("test1", "test2");
        assertLogMessage("{\n"
            + "  \"name\" : \"test1\",\n"
            + "  \"numPartitions\" : 8,\n"
            + "  \"replicationFactor\" : 1,\n"
            + "  \"replicasAssignments\" : null,\n"
            + "  \"configs\" : {\n"
            + "    \"cleanup.policy\" : \"delete\",\n"
            + "    \"retention.ms\" : \"1000\"\n"
            + "  }\n"
            + "}");
        assertLogMessage("{\n"
            + "  \"name\" : \"test2\",\n"
            + "  \"numPartitions\" : 4,\n"
            + "  \"replicationFactor\" : 1,\n"
            + "  \"replicasAssignments\" : null,\n"
            + "  \"configs\" : {\n"
            + "    \"cleanup.policy\" : \"delete\",\n"
            + "    \"retention.ms\" : \"2000\"\n"
            + "  }\n"
            + "}");
    }

    @Configuration
    @ImportAutoConfiguration(TopicProvisionAutoConfiguration.class)
    static class EmptyConfiguration {

    }
}