package io.github.zghurskyi.kafka;

import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

final class TestContextLoader {

    private TestContextLoader() {
        throw new UnsupportedOperationException("Instantiation is not supported!");
    }

    static ConfigurableApplicationContext load(Class<?> config, String... environment) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(config);
        TestPropertyValues.of(environment).applyTo(context);
        context.refresh();
        return context;
    }
}