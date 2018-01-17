package com.github.rocketmq.annotation;

import com.github.rocketmq.base.MessageExtConst;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;


@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface MQConsumer {

    String consumerGroup();
    String topic();

    /**
     * 广播模式消费： BROADCASTING
     * 集群模式消费： CLUSTERING
     * @return 消息模式
     */
    String messageMode() default MessageExtConst.MESSAGE_MODE_CLUSTERING;

    /**
     * 使用线程池并发消费: CONCURRENTLY("CONCURRENTLY"),
     * 单线程消费: ORDERLY("ORDERLY");
     * @return 消费模式
     */
    String consumeMode() default MessageExtConst.CONSUME_MODE_CONCURRENTLY;

    String[] tag() default {"*"};
}
