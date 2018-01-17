package com.github.rocketmq.test;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * @author Stephen
 * @since 2018-01-16
 */
public class ProducerTest {

    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    @Test
    public void testSyncSend() throws Exception {


        DefaultMQProducer producer = new DefaultMQProducer();

        producer.setNamesrvAddr("192.168.33.10:9876");
        producer.setProducerGroup("producer_group");
        producer.setVipChannelEnabled(false);

        producer.start();


        Message message = new Message();
        message.setTopic("TEST_TOPIC");
        message.setTags("tag_a");
        message.setBody("hello sync send, hell and shit".getBytes());

        SendResult result = producer.send(message);


        System.out.println("result == " + result);

    }

    @Test
    public void testPushConsume() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        consumer.setNamesrvAddr("192.168.33.10:9876");
        consumer.setConsumerGroup("consume_group");
        consumer.setVipChannelEnabled(false);

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe("TEST_TOPIC", "*");


        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
                for (MessageExt ext : messages) {

                    System.out.println("message: " + new String(ext.getBody()));
                    System.out.println("message id: " + ext.getMsgId());
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

    }

    @Test
    public void testPullConsume() throws Exception {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer();
        consumer.setNamesrvAddr("192.168.33.10:9876");
        consumer.setConsumerGroup("consume_group");
        consumer.setVipChannelEnabled(false);

        consumer.start();

        Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues("TEST_TOPIC");

        for (MessageQueue queue : queues) {
            SINGLE_MQ:
            while (true) {
                try {
                    PullResult pullResult = consumer.pullBlockIfNotFound(queue, null, getMessageQueueOffset(queue), 32);
                    System.out.printf("%s%n", pullResult);
                    putMessageQueueOffset(queue, pullResult.getNextBeginOffset());

                    List<MessageExt> messages = pullResult.getMsgFoundList();
                    if (messages != null)
                    for (MessageExt ext : messages) {

                        System.out.println("message: " + new String(ext.getBody()));
                        System.out.println("message id: " + ext.getMsgId());
                    }
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            break SINGLE_MQ;
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }





    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }
}
