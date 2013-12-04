package com.rocketmq.community.jms;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.rocketmq.community.jms.message.MessageBase;
import com.rocketmq.community.jms.util.JMSExceptionSupport;

import javax.jms.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MQMessageProducer implements MessageProducer {
    protected MQProducer targetProducer;
    protected Destination destination;
    protected MQSession session;
    protected AtomicBoolean started = new AtomicBoolean(false);

    MQMessageProducer(MQSession session, MQProducer producer, Destination dest) throws JMSException {
        this.session = session;
        targetProducer = producer;
        destination = dest;
        session.addProducer(this);
        if (session.isStarted()) {
            start();
        }
    }

    @Override
    public void setDisableMessageID(boolean value) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean getDisableMessageID() throws JMSException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getDeliveryMode() throws JMSException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setPriority(int defaultPriority) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getPriority() throws JMSException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setTimeToLive(long timeToLive) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getTimeToLive() throws JMSException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Destination getDestination() throws JMSException {
        return destination;
    }

    @Override
    public void close() throws JMSException {
        try {
            if (started.get()) {
                targetProducer.shutdown();
                targetProducer = null;
                session.removeProducer(this);
                started.set(false);
            }
        } catch (Exception ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }

    @Override
    public void send(Message message) throws JMSException {
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException {
        send(destination, message, 0, 0, 0);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        message.setJMSDestination(destination);
        com.alibaba.rocketmq.common.message.Message convertedMsg = ((MessageBase)message).convert();
        try {
            if (!started.get()) {
                targetProducer.start();
                started.set(true);
            }

            targetProducer.send(convertedMsg);
        } catch (Exception ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }

    public void start() throws JMSException {
        try {
            if (!started.get()) {
                targetProducer.start();
                started.set(true);
            }
        } catch (MQClientException ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }
}
