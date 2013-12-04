package com.rocketmq.community.jms;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.rocketmq.community.jms.message.*;
import com.rocketmq.community.jms.util.JMSExceptionSupport;

import javax.jms.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class MQSession implements QueueSession, TopicSession {
    private MQConnection connection;
    int acknowledgeMode;
    private List<MQMessageProducer> producers;
    private List<MQMessageConsumer> consumers;
    private AtomicBoolean started = new AtomicBoolean(false);


    public MQSession(MQConnection connection, int acknowledgeMode) throws JMSException {
        this.connection = connection;
        this.acknowledgeMode = acknowledgeMode;
        producers = new CopyOnWriteArrayList<MQMessageProducer>();
        consumers = new CopyOnWriteArrayList<MQMessageConsumer>();
        connection.addSession(this);

        if (connection.isStarted()) {
            start();
        }
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        try {
            return new MQTopicPublisher(this, createTargetProducer(topic), topic);
        } catch (Exception ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }

    // Implement Session
    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        return new BytesMessageImpl();
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        return new MapMessageImpl();
    }

    @Override
    public Message createMessage() throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        return new ObjectMessageImpl();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        return new StreamMessageImpl();
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        TextMessageImpl message = new TextMessageImpl(text, true);
        return message;
    }

    @Override
    public boolean getTransacted() throws JMSException {
        // TODO 添加具体实现
        return false;
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        return acknowledgeMode;
    }

    @Override
    public void commit() throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public void rollback() throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public void close() throws JMSException {
        for (MessageProducer producer : producers) {
            producer.close();
        }
        producers.clear();

        for (MessageConsumer consumer : consumers) {
            consumer.close();
        }
        consumers.clear();

        started.set(false);
    }

    protected void start() throws JMSException {
        if (!isStarted()) {
            for (MQMessageProducer producer : producers) {
                producer.start();
            }

            for (MQMessageConsumer consumer : consumers) {
                consumer.start();
            }

            started.set(true);
        }
    }

    @Override
    public void recover() throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public void run() {
        return;
    }

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        try {
            return new MQMessageProducer(this, createTargetProducer(destination), destination);
        } catch (Exception ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }

    private MQProducer createTargetProducer(Destination destination) throws JMSException {
        MQProducer producer;

        if (destination == null) {
            throw new JMSException("Destination can not be null.");
        }

        String name = destination.toString() + "_" + UUID.randomUUID();

        if (!getTransacted()) {
            producer = new DefaultMQProducer(destination.toString());
        } else {
            producer = new TransactionMQProducer(destination.toString());
        }

        // TransactionMQProducer继承DefaultMQProducer，所以这么来设instance name.
        if (((DefaultMQProducer)producer).getInstanceName().equalsIgnoreCase("DEFAULT")) {
            // instance name没被人为设置过
            ((DefaultMQProducer)producer).setInstanceName(name);
        }

        return producer;
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return createConsumer(destination, null);
    }

    @Override
    public MessageConsumer createConsumer(
            Destination destination,
            String messageSelector)
            throws JMSException {
        String name = destination.toString() + "_" + UUID.randomUUID();
        DefaultMQPullConsumer target = new DefaultMQPullConsumer(destination.toString());

        if (target.getInstanceName().equalsIgnoreCase("DEFAULT")) {
            // instance name没被人为设置过
            target.setInstanceName(name);
        }

        if (destination instanceof Topic) {
            target.setMessageModel(MessageModel.BROADCASTING);
        } else {
            target.setMessageModel(MessageModel.CLUSTERING);
        }

        return new MQMessageConsumer(this, target, destination.toString(), messageSelector);
    }

    @Override
    public MessageConsumer createConsumer(
            Destination destination,
            String messageSelector,
            boolean NoLocal)
            throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public Queue createQueue(String queueName) throws JMSException {
        return new MQQueue(queueName);
    }

    @Override
    public Topic createTopic(String topicName) throws JMSException {
        return new MQTopic(topicName);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name)
            throws JMSException {
        return createDurableSubscriber(topic, name, null, false);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(
            Topic topic,
            String name,
            String messageSelector,
            boolean noLocal)
            throws JMSException {
        DefaultMQPushConsumer target = new DefaultMQPushConsumer(name);
        target.setInstanceName(name);
        target.setMessageModel(MessageModel.BROADCASTING);

        return new MQTopicSubscriber(this, target, topic.toString(), messageSelector);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector)
            throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public void unsubscribe(String name) throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    // Implement QueueSession

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        throw new JMSException(Thread.currentThread().getStackTrace()[1].getMethodName() + ": Not supported");
    }

    public void addProducer(MQMessageProducer producer) {
        producers.add(producer);
    }

    public void removeProducer(MQMessageProducer producer) {
        producers.remove(producer);
    }

    public void addConsumer(MQMessageConsumer consumer) {
        consumers.add(consumer);
    }

    public void removeConsumer(MQMessageConsumer consumer) {
        consumers.remove(consumer);
    }

    public Boolean isStarted() {
        return started.get();
    }
}
