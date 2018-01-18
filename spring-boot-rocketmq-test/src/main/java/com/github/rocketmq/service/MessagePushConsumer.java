package com.github.rocketmq.service;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Stephen
 * @since 2018-01-18
 */
@Service
public class MessagePushConsumer implements CommandLineRunner {

    @Autowired
    private DefaultMQPushConsumer pushConsumer;

    @Override
    public void run(String... args) throws Exception {
        pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        pushConsumer.subscribe("PUSH_TEST_TOPIC", "push_tag");


        pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
                for (MessageExt ext : messages) {

                    System.out.println("push message: " + new String(ext.getBody()));
                    System.out.println("push message id: " + ext.getMsgId());
                    System.out.println("push tags: " + ext.getTags());
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        pushConsumer.start();
    }
}
