package com.rocketmq.community.jms;

import javax.jms.JMSException;
import javax.jms.Topic;

public class MQTopic extends MQDestination implements Topic {
    public MQTopic(String name) {
        super(name);
    }
    public String getTopicName() throws JMSException {
        return name;
    }

    public String toString() {
        return "Topic_" + name;
    }
}
