package com.rocketmq.community.jms;

import com.alibaba.rocketmq.client.consumer.MQConsumer;
import com.alibaba.rocketmq.client.consumer.MQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.rocketmq.community.jms.message.MessageBase;
import com.rocketmq.community.jms.util.JMSExceptionSupport;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

/**
 * Created with IntelliJ IDEA.
 * User: CalvinZhan
 * Date: 11/25/13
 * Time: 2:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class MQTopicSubscriber extends MQMessageConsumer implements TopicSubscriber {
    public MQTopicSubscriber(MQSession session, MQPushConsumer consumer, String topic, String tag) throws JMSException {
        super(session, consumer, topic, tag);

        try {
            consumer.subscribe(topic, MessageBase.JMS_SOURCE + tag);
        } catch (MQClientException ex) {
            throw JMSExceptionSupport.create(ex);
        }

    }

    @Override
    public Topic getTopic() throws JMSException {
        return new MQTopic(topic);
    }

    @Override
    public boolean getNoLocal() throws JMSException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
