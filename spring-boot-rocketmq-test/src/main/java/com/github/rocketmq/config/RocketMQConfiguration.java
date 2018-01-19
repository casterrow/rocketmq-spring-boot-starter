package com.github.rocketmq.config;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Stephen
 * @since 2018-01-18
 */
@Configuration
public class RocketMQConfiguration {


    @Bean
    public DefaultMQProducer producer() throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setNamesrvAddr("192.168.33.10:9876");
        producer.setProducerGroup("producer_group");
        producer.setVipChannelEnabled(false);
        producer.start();
        return producer;
    }


    @Bean
    public DefaultMQPushConsumer pushConsumer() {
        DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer();
        pushConsumer.setNamesrvAddr("192.168.33.10:9876");
        pushConsumer.setConsumerGroup("push_consumer_group");
        pushConsumer.setVipChannelEnabled(false);
        return pushConsumer;

    }

    @Bean
    public DefaultMQPullConsumer pullConsumer() {
        DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer();
        pullConsumer.setNamesrvAddr("192.168.33.10:9876");
        pullConsumer.setConsumerGroup("pull_consumer_group");
        pullConsumer.setVipChannelEnabled(false);
        return pullConsumer;
    }

    @Bean
    public MQPullConsumerScheduleService scheduleService() {
        DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer();
        pullConsumer.setNamesrvAddr("192.168.33.10:9876");
        pullConsumer.setConsumerGroup("schedule_consumer_group");
        pullConsumer.setVipChannelEnabled(false);
        MQPullConsumerScheduleService scheduleService = new MQPullConsumerScheduleService("schedule_consumer_group");
        scheduleService.setMessageModel(MessageModel.CLUSTERING);
        scheduleService.setDefaultMQPullConsumer(pullConsumer);
        return scheduleService;
    }
}
