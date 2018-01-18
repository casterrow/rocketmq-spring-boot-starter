package com.github.rocketmq.controller;

import com.alibaba.fastjson.JSON;
import com.github.rocketmq.model.Product;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @author Stephen
 * @since 2018-01-18
 */
@RestController
@RequestMapping("/message")
public class MessageController {


    @Autowired
    private DefaultMQProducer producer;

    @PostMapping("/syncSendForPull")
    public void syncSendForPull(@RequestBody Product product) throws Exception {
        Message message = new Message();
        message.setTopic("PULL_TEST_TOPIC");
        message.setTags(product.getTags());
        message.setBody(JSON.toJSONBytes(product));

        SendResult result = producer.send(message);

        System.out.println("result: " + result);
    }

    @PostMapping("/syncSendForPush")
    public void syncSendForPush(@RequestBody Product product) throws Exception {
        Message message = new Message();
        message.setTopic("PUSH_TEST_TOPIC");
        message.setTags(product.getTags());
        message.setBody(JSON.toJSONBytes(product));

        SendResult result = producer.send(message);

        System.out.println("result: " + result);
    }
}
