package io.github.zghurskyi.kafka;

import io.github.zghurskyi.kafka.validation.TopicName;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "kafka.provision")
@Validated
public class ProvisionProperties {

    private String brokers;
    private boolean enabled = true;
    private boolean autoCreateTopics = true;
    private boolean autoAddPartitions = true;
    private boolean autoUpdateConfig = true;
    private List<@Valid TopicProperties> topics = new ArrayList<>();
    @Valid
    private ProvisionRetryProperties provisionRetry = new ProvisionRetryProperties();

    public ProvisionProperties() {
    }

    public String getBrokers() {
        return this.brokers;
    }

    public boolean isEnabled() {
        return this.enabled;
    }

    public boolean isAutoCreateTopics() {
        return this.autoCreateTopics;
    }

    public boolean isAutoAddPartitions() {
        return this.autoAddPartitions;
    }

    public boolean isAutoUpdateConfig() {
        return this.autoUpdateConfig;
    }

    public List<@Valid TopicProperties> getTopics() {
        return this.topics;
    }

    public @Valid ProvisionRetryProperties getProvisionRetry() {
        return this.provisionRetry;
    }

    public void setBrokers(String brokers) {
        this.brokers = brokers;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setAutoCreateTopics(boolean autoCreateTopics) {
        this.autoCreateTopics = autoCreateTopics;
    }

    public void setAutoAddPartitions(boolean autoAddPartitions) {
        this.autoAddPartitions = autoAddPartitions;
    }

    public void setAutoUpdateConfig(boolean autoUpdateConfig) {
        this.autoUpdateConfig = autoUpdateConfig;
    }

    public void setTopics(List<@Valid TopicProperties> topics) {
        this.topics = topics;
    }

    public void setProvisionRetry(@Valid ProvisionRetryProperties provisionRetry) {
        this.provisionRetry = provisionRetry;
    }

    public static class TopicProperties {
        @TopicName
        private String name;
        @Min(1)
        private int numPartitions;
        @Min(1)
        private short replicationFactor;
        private Map<String, String> configs = new HashMap<>();

        public TopicProperties() {
        }

        @TopicName
        public String getName() {
            return this.name;
        }

        @Min(1)
        public int getNumPartitions() {
            return this.numPartitions;
        }

        @Min(1)
        public short getReplicationFactor() {
            return this.replicationFactor;
        }

        public Map<String, String> getConfigs() {
            return this.configs;
        }

        public void setName(@TopicName String name) {
            this.name = name;
        }

        public void setNumPartitions(@Min(1) int numPartitions) {
            this.numPartitions = numPartitions;
        }

        public void setReplicationFactor(@Min(1) short replicationFactor) {
            this.replicationFactor = replicationFactor;
        }

        public void setConfigs(Map<String, String> configs) {
            this.configs = configs;
        }
    }

    public static class ProvisionRetryProperties {

        static final int DEFAULT_MAX_ATTEMPTS = 3;
        static final long DEFAULT_INITIAL_INTERVAL_MILLIS = 100L;
        static final int DEFAULT_BACKOFF_MULTIPLIER = 2;
        static final long DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS = 30000L;

        @Min(1) @Max(10)
        private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
        @Min(100) @Max(10000)
        private long initialIntervalMillis = DEFAULT_INITIAL_INTERVAL_MILLIS;
        @Min(1) @Max(3)
        private double multiplier = DEFAULT_BACKOFF_MULTIPLIER;
        @Min(1) @Max(60000)
        private long maxIntervalMillis = DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS;

        public ProvisionRetryProperties() {
        }

        @Min(1) @Max(10)
        public int getMaxAttempts() {
            return this.maxAttempts;
        }

        @Min(100) @Max(10000)
        public long getInitialIntervalMillis() {
            return this.initialIntervalMillis;
        }

        @Min(1) @Max(3)
        public double getMultiplier() {
            return this.multiplier;
        }

        @Min(1) @Max(60000)
        public long getMaxIntervalMillis() {
            return this.maxIntervalMillis;
        }

        public void setMaxAttempts(@Min(1) @Max(10) int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public void setInitialIntervalMillis(@Min(100) @Max(10000) long initialIntervalMillis) {
            this.initialIntervalMillis = initialIntervalMillis;
        }

        public void setMultiplier(@Min(1) @Max(3) double multiplier) {
            this.multiplier = multiplier;
        }

        public void setMaxIntervalMillis(@Min(1) @Max(60000) long maxIntervalMillis) {
            this.maxIntervalMillis = maxIntervalMillis;
        }
    }
}