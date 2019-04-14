package io.github.zghurskyi.kafka;

import io.github.zghurskyi.kafka.client.AdminClientFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
@ConditionalOnClass(AdminClient.class)
@ConditionalOnProperty(value = "kafka.provision.enabled", matchIfMissing = true)
@AutoConfigureAfter(KafkaAutoConfiguration.class)
@EnableConfigurationProperties(ProvisionProperties.class)
public class TopicProvisionAutoConfiguration {

    private static final String KAFKA_PROVISION_BROKERS_PROPERTY = "kafka.provision.brokers";

    @Bean
    @ConditionalOnProperty(KAFKA_PROVISION_BROKERS_PROPERTY)
    public TopicProvisioner provisioner(ProvisionProperties properties, AdminClientFactory clientFactory,
                                        RetryTemplate retryTemplate) {
        return new TopicProvisioner(properties, clientFactory, retryTemplate);
    }

    @Bean
    @ConditionalOnProperty(KAFKA_PROVISION_BROKERS_PROPERTY)
    public AdminClientFactory clientFactory(ProvisionProperties properties) {
        return new AdminClientFactory(properties.getBrokers());
    }

    @Bean
    @ConditionalOnProperty(KAFKA_PROVISION_BROKERS_PROPERTY)
    public RetryTemplate retryTemplate(ProvisionProperties properties) {
        ProvisionProperties.ProvisionRetryProperties provisionRetryProperties = properties.getProvisionRetry();

        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        simpleRetryPolicy.setMaxAttempts(provisionRetryProperties.getMaxAttempts());

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(provisionRetryProperties.getInitialIntervalMillis());
        backOffPolicy.setMultiplier(provisionRetryProperties.getMultiplier());
        backOffPolicy.setMaxInterval(provisionRetryProperties.getMaxIntervalMillis());

        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy);
        retryTemplate.setBackOffPolicy(backOffPolicy);

        return retryTemplate;
    }
}
