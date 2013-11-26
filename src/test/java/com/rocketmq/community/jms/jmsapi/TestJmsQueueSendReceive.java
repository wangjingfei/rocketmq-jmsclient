package com.rocketmq.community.jms.jmsapi;

import com.rocketmq.community.jms.MQQueue;
import com.rocketmq.community.jms.MQTopic;
import org.junit.Test;

import javax.jms.Destination;

public class TestJmsQueueSendReceive extends JmsSendReceiveTestSupport {
    @Test
    public void testSendReceive() throws Exception {
        super.testSendReceive();
    }

    @Override
    protected Destination createDestination(String subject) {
        return new MQQueue(subject);
    }
}
