package com.github.rocketmq.service;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Stephen
 * @since 2018-01-18
 */
@Service
public class MessagePullConsumer implements CommandLineRunner {

    @Autowired
    private DefaultMQPullConsumer pullConsumer;

    private static final Map<MessageQueue, Long> OFFSET_TABLE = new HashMap<>();

    @Override
    public void run(String... args) throws Exception {
        pullConsumer.start();
        Set<MessageQueue> queues = pullConsumer.fetchSubscribeMessageQueues("PULL_TEST_TOPIC");

        for (MessageQueue queue : queues) {
            SINGLE_MQ:
            while (true) {
                try {
                    PullResult pullResult = pullConsumer.pullBlockIfNotFound(queue, null, getMessageQueueOffset(queue), 32);
                    putMessageQueueOffset(queue, pullResult.getNextBeginOffset());
                    List<MessageExt> messages = pullResult.getMsgFoundList();
                    if (messages != null) {
                        for (MessageExt ext : messages) {
                            System.out.println("pull message: " + new String(ext.getBody()));
                            System.out.println("pull message id: " + ext.getMsgId());
                            System.out.println("pull tags: " + ext.getTags());
                        }
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


    private long getMessageQueueOffset(MessageQueue queue) {
        Long offset = OFFSET_TABLE.get(queue);
        if (offset != null) {
            return offset;
        }
        return 0;
    }

    private void putMessageQueueOffset(MessageQueue queue, long offset) {
        OFFSET_TABLE.put(queue, offset);
    }
}
