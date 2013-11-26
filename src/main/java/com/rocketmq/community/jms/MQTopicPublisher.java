package com.rocketmq.community.jms;

import com.alibaba.rocketmq.client.producer.MQProducer;

import javax.jms.*;

public class MQTopicPublisher extends MQMessageProducer implements TopicPublisher{

    public MQTopicPublisher(MQSession session, MQProducer producer, Destination dest) throws JMSException {
        super(session, producer, dest);
    }

    @Override
    public Topic getTopic() throws JMSException {
        return (Topic)destination;
    }

    @Override
    public void publish(Message message) throws JMSException {
        send(message);
    }

    @Override
    public void publish(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void publish(Topic topic, Message message) throws JMSException {
        send(topic, message);
    }

    @Override
    public void publish(Topic topic, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(topic, message, deliveryMode, priority, timeToLive);
    }
}
