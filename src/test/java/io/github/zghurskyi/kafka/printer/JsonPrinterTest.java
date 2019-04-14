package io.github.zghurskyi.kafka.printer;

import io.github.zghurskyi.kafka.ProvisionProperties;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonPrinterTest {

    private static final String NO_TOPICS_JSON = "{\n"
        + "  \"brokers\" : null,\n"
        + "  \"enabled\" : true,\n"
        + "  \"autoCreateTopics\" : true,\n"
        + "  \"autoAddPartitions\" : true,\n"
        + "  \"autoUpdateConfig\" : true,\n"
        + "  \"topics\" : [ ],\n"
        + "  \"provisionRetry\" : {\n"
        + "    \"maxAttempts\" : 3,\n"
        + "    \"initialIntervalMillis\" : 100,\n"
        + "    \"multiplier\" : 2.0,\n"
        + "    \"maxIntervalMillis\" : 30000\n"
        + "  }\n"
        + "}";

    @Test
    public void printsTopicConfigAsJson() {
        ProvisionProperties properties = newNoTopicsProperties();
        String json = JsonPrinter.print(properties);
        assertThat(json).isEqualTo(NO_TOPICS_JSON);
    }

    private ProvisionProperties newNoTopicsProperties() {
        ProvisionProperties properties = new ProvisionProperties();
        properties.setTopics(Collections.emptyList());
        return properties;
    }
}