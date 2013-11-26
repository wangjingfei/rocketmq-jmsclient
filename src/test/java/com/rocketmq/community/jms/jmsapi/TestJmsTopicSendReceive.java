package com.rocketmq.community.jms.jmsapi;

import com.rocketmq.community.jms.MQTopic;
import com.rocketmq.community.jms.message.MessageBase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.UUID;

public class TestJmsTopicSendReceive extends JmsSendReceiveTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(TestJmsTopicSendReceive.class);
    protected TopicPublisher publisher;
    protected TopicSubscriber subscriber;

    protected void setUp() throws Exception {
        super.setUp();
        publisher = ((TopicSession)session).createPublisher((Topic)producerDestination);
        LOG.info("Created publisher: " + publisher);
    }

    @Test
    public void testSendReceive() throws Exception {
        super.testSendReceive();
    }

    @Test
    public void testPubSub_2Subs() throws Exception {
        String msgTag = UUID.randomUUID().toString();
        subscriber = createSubscriber(msgTag);
        subscriber.setMessageListener(this);
        TopicSubscriber subscriber2 = createSubscriber(msgTag);
        subscriber2.setMessageListener(this);
        connection.start();
        messages.clear();

        for (int i = 0; i < data.length; i++) {
            Message message = session.createTextMessage(data[i]);
            message.setStringProperty(MessageBase.MSG_TAG_NAME, msgTag);
            message.setStringProperty("stringProperty", data[i]);
            message.setIntProperty("intProperty", i);

            if (verbose) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("About to send a message: " + message + " with text: " + data[i]);
                }
            }

            publisher.publish((Topic)producerDestination, message);
        }

        for (int i = 0; i < 2; i++) {
            synchronized (lock) {
                lock.wait();
            }
        }
    }

    protected TopicSubscriber createSubscriber(String messageSelector) throws JMSException {
        LOG.info("Creating durable subscriber");
        return session.createDurableSubscriber((Topic)consumerDestination, UUID.randomUUID().toString(), messageSelector, true);
    }


    @Override
    protected Destination createDestination(String subject) {
        return new MQTopic(subject);
    }
}
