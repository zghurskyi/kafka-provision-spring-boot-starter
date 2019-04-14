package io.github.zghurskyi.kafka.validation;

import javax.validation.Constraint;
import javax.validation.Payload;
import javax.validation.constraints.Pattern;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;

@Pattern(regexp = "[a-zA-Z0-9._\\-]{1,249}")
@Target({FIELD, PARAMETER, TYPE_USE})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = {})
public @interface TopicName {

    String message() default "{io.github.zghurskyi.kafka.validation.TopicName.message}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
