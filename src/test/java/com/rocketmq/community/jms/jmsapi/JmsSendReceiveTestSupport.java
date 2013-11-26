package com.rocketmq.community.jms.jmsapi;

import com.rocketmq.community.jms.MQConnectionFactory;
import com.rocketmq.community.jms.MQQueue;
import com.rocketmq.community.jms.MQTopic;
import com.rocketmq.community.jms.message.MessageBase;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.*;

public abstract class JmsSendReceiveTestSupport extends TestBase implements MessageListener {
    private static final Logger LOG = LoggerFactory.getLogger(JmsSendReceiveTestSupport.class);

    protected int messageCount = 10;
    protected String[] data;
    protected Session session;
    protected MessageConsumer consumer;
    protected MessageProducer producer;
    protected Destination consumerDestination;
    protected Destination producerDestination;
    protected List<Message> messages = createConcurrentList();
    protected boolean durable;
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    protected final Object lock = new Object();
    protected boolean verbose;
    protected Connection connection;
    protected MQConnectionFactory connectionFactory;

    protected void setUp() throws Exception {
        super.setUp();
        String temp = System.getProperty("messageCount");

        if (temp != null) {
            int i = Integer.parseInt(temp);
            if (i > 0) {
                messageCount = i;
            }
        }

        LOG.info("Message count for test case is: " + messageCount);
        data = new String[messageCount];

        for (int i = 0; i < messageCount; i++) {
            data[i] = "Text for message: " + i + " at " + new Date();
        }

        connectionFactory = createConnectionFactory();
        connection = createConnection();
        if (durable) {
            connection.setClientID(getClass().getName());
        }
        LOG.info("Created connection: " + connection);

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        LOG.info("Created session: " + session);

        consumerDestination = createDestination(getConsumerSubject());
        producerDestination = createDestination(getProducerSubject());

        LOG.info("Created  consumer destination: " + consumerDestination + " of type: " + consumerDestination.getClass());
        LOG.info("Created  producer destination: " + producerDestination + " of type: " + producerDestination.getClass());

        producer = session.createProducer(producerDestination);
        producer.setDeliveryMode(deliveryMode);
        LOG.info("Created producer: " + producer + " delivery mode = " + (deliveryMode == DeliveryMode.PERSISTENT ? "PERSISTENT" : "NON_PERSISTENT"));
    }

    public void testSendReceive() throws Exception {
        String msgTag = UUID.randomUUID().toString();
        consumer = createConsumer(msgTag);
        consumer.setMessageListener(this);
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

            sendToProducer(producer, producerDestination, message);
        }

        for (int i = 0; i < data.length; i++) {
            Message msg = consumer.receive(5000);
            if (msg instanceof TextMessage) {
                TextMessage replyMessage = (TextMessage)msg;
                LOG.info("Received reply.");
                LOG.info(replyMessage.toString());
                assertEquals("Wrong message content", ((TextMessage) msg).getText(), replyMessage.getText());
                messages.add(replyMessage);
            } else {
                fail("Should have received a reply by now");
            }

        }

        producer.close();
        consumer.close();
        assertMessagesAreReceived();
        LOG.info("" + data.length + " messages(s) received, closing down connections");
    }

    protected void sendToProducer(MessageProducer producer,
                                  Destination producerDestination, Message message) throws JMSException {
        producer.send(producerDestination, message);
    }


    protected void assertMessagesAreReceived() throws JMSException {
        waitForMessagesToBeDelivered();
        assertMessagesReceivedAreValid(messages);
    }


    protected void assertMessagesReceivedAreValid(List<Message> receivedMessages) throws JMSException {
        List<Object> copyOfMessages = Arrays.asList(receivedMessages.toArray());
        int counter = 0;

        if (data.length != copyOfMessages.size()) {
            for (Iterator<Object> iter = copyOfMessages.iterator(); iter.hasNext();) {
                TextMessage message = (TextMessage)iter.next();
                if (LOG.isInfoEnabled()) {
                    LOG.info("<== " + counter++ + " = " + message.getText());
                }
            }
        }

        assertEquals("Not enough messages received", data.length, receivedMessages.size());

        for (int i = 0; i < data.length; i++) {
            TextMessage received = (TextMessage)receivedMessages.get(i);
            String text = received.getText();
            String stringProperty = received.getStringProperty("stringProperty");
            int intProperty = received.getIntProperty("intProperty");

            if (verbose) {
                if (LOG.isDebugEnabled()) {
                    LOG.info("Received Text: " + text);
                }
            }

//            assertEquals("Message: " + i, data[i], text);
//            assertEquals(data[i], stringProperty);
//            assertEquals(i, intProperty);
        }
    }

    protected void waitForMessagesToBeDelivered() {
        long maxWaitTime = 60000;
        long waitTime = maxWaitTime;
        long start = (maxWaitTime <= 0) ? 0 : System.currentTimeMillis();

        synchronized (lock) {
            while (messages.size() < data.length && waitTime >= 0) {
                try {
                    lock.wait(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                waitTime = maxWaitTime - (System.currentTimeMillis() - start);
            }
        }
    }


    public synchronized void onMessage(Message message) {
        consumeMessage(message, messages);
    }

    private int counter;
    protected void consumeMessage(Message message, List<Message> messageList) {
        LOG.info("Received message: " + message);

        messageList.add(message);
        counter++;
        if (counter >= data.length) {
            synchronized (lock) {
                counter = 0;
                lock.notifyAll();
            }
        }
    }

    protected List<Message> createConcurrentList() {
        return Collections.synchronizedList(new ArrayList<Message>());
    }

    /**
     * Just a hook so can insert failure tests
     *
     * @throws Exception
     */
    protected void messageSent() throws Exception {

    }

    protected MQConnectionFactory createConnectionFactory() throws Exception {
        return new MQConnectionFactory();
    }

    protected Connection createConnection() throws Exception {
        return getConnectionFactory().createConnection();
    }

    public MQConnectionFactory getConnectionFactory() throws Exception {
        if (connectionFactory == null) {
            connectionFactory = createConnectionFactory();
            Assert.assertTrue("Should have created a connection factory!", connectionFactory != null);
        }

        return connectionFactory;
    }

    protected String getConsumerSubject() {
        return getSubject();
    }

    protected String getProducerSubject() {
        return getSubject();
    }

    protected String getSubject() {
        return getClass().getName().replace(".", "_");
    }

    protected abstract Destination createDestination(String subject);
    protected abstract MessageConsumer createConsumer(String messageSelector) throws JMSException;
}
