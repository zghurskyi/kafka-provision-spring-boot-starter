package io.github.zghurskyi.kafka.printer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JsonPrinter {

    private static final Logger log = LoggerFactory.getLogger(JsonPrinter.class);

    private JsonPrinter() {
        throw new UnsupportedOperationException("Instantiation is not supported!");
    }

    public static <T> String print(T value) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        try {
            return objectMapper
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(value);
        } catch (JsonProcessingException e) {
            log.warn("Failed to pretty-print: " + e.getMessage(), e);
            return value.toString();
        }
    }
}
