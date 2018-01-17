package com.github.rocketmq.annotation;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;


@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface MQProducer {
    String topic() default "";
    String tag() default "";
}
