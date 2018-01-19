package com.github.rocketmq.service;

import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Stephen
 * @since 2018-01-19
 */
@Service
public class MessagePullScheduleConsumer implements CommandLineRunner {

    @Autowired
    private MQPullConsumerScheduleService scheduleService;

    @Override
    public void run(String... args) throws Exception {

        scheduleService.registerPullTaskCallback("TASK_TEST_TOPIC", (messageQueue, context) -> {

            MQPullConsumer consumer = context.getPullConsumer();

            try {
                long offset = consumer.fetchConsumeOffset(messageQueue, false);
                if (offset < 0) {
                    offset = 0;
                }

                PullResult result = consumer.pull(messageQueue, "*", offset, 32);

                //获取消息
                List<MessageExt> messages = result.getMsgFoundList();
                if (messages != null) {
                    messages.forEach(x -> {
                        System.out.println("task body: " + new String(x.getBody()));
                        System.out.println("task message id: " + x.getMsgId());
                        System.out.println("task tags: " + x.getTags());
                    });
                }
                switch (result.getPullStatus()) {
                    case FOUND:
                        break;
                    case NO_NEW_MSG:
                    case NO_MATCHED_MSG:
                        break;
                    case OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                }

                //更新队列offset
                consumer.updateConsumeOffset(messageQueue, result.getNextBeginOffset());

                //设置多久拉取一次
                context.setPullNextDelayTimeMillis(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }


        });

        scheduleService.start();
    }
}
