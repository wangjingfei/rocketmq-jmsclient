package com.rocketmq.community.jms.spring;

import com.alibaba.rocketmq.broker.BrokerStartup;
import com.alibaba.rocketmq.namesrv.NamesrvStartup;
import com.rocketmq.community.jms.helper.JmsConsumerAsync;
import com.rocketmq.community.jms.helper.JmsProducer;
import com.rocketmq.community.jms.helper.StepEnum;
import com.rocketmq.community.jms.helper.TestObject;
import com.rocketmq.community.jms.message.BytesMessageImpl;
import com.rocketmq.community.jms.message.MapMessageImpl;
import com.rocketmq.community.jms.message.ObjectMessageImpl;
import com.rocketmq.community.jms.message.StreamMessageImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class TestQueueListener {
    private JmsConsumerAsync messageListener;
    private JmsProducer producer;
    private Object preparationLock = new Object();
    private Message receivedMsg;
    private static boolean nameSrvStarted = false;
    private static boolean brokerStarted = false;
    private static ApplicationContext ctx = new ClassPathXmlApplicationContext("queue-producer-context.xml", "queue-consumer-context.xml");

    @Before
    public void setup() {
        // Name Server, Broker会在case执行完被关闭（因为JVM的关闭，它们的shutdown hook会被调用)
        if (!nameSrvStarted) {
            NamesrvStartup.main(null);
            nameSrvStarted = true;
        }

        if (!brokerStarted) {
            BrokerStartup.main(null);
            brokerStarted = true;
        }
    }


    @Test
    public void TestSendQueueTextMessagge_MultiThread() throws JMSException {
        sendReceiveMessageMultiThread(MessageType.TextMessage);

        Assert.assertEquals(producer.textMessage, ((TextMessage) receivedMsg).getText());
    }

    @Test
    public void TestSendQueueMapMessagge_MultiThread() throws JMSException {
        sendReceiveMessageMultiThread(MessageType.MapMessage);

        Assert.assertEquals((short)producer.mapDeptIdValue, ((MapMessageImpl)receivedMsg).getShort(producer.mapDeptId));
        Assert.assertEquals(producer.mapSideValue, ((MapMessageImpl)receivedMsg).getString(producer.mapSide));
        Assert.assertEquals((long)producer.mapAcctIdValue, ((MapMessageImpl)receivedMsg).getLong(producer.mapAcctId));
        Assert.assertEquals(producer.mapSharesValue, ((MapMessageImpl)receivedMsg).getDouble(producer.mapShares), 0);
        Assert.assertEquals((char)producer.mapSexValue, ((MapMessageImpl)receivedMsg).getChar(producer.mapSex));
        Assert.assertNull(((MapMessageImpl)receivedMsg).getString("NONE"));
        Assert.assertEquals(0.0, ((MapMessageImpl)receivedMsg).getDouble("NONE"), 0);

    }

    @Test
    public void TestSendQueueBytesMessagge_MultiThread() throws JMSException {
        sendReceiveMessageMultiThread(MessageType.BytesMessage);

        Assert.assertEquals(producer.double1, ((BytesMessageImpl) receivedMsg).readDouble(), 0);
        Assert.assertEquals(producer.double2, ((BytesMessageImpl)receivedMsg).readDouble(), 0);
        Assert.assertEquals(producer.character, ((BytesMessageImpl)receivedMsg).readChar());
        Assert.assertEquals(producer.utf, ((BytesMessageImpl)receivedMsg).readUTF());
    }

    @Test
    public void TestSendQueueStreamMessagge_MultiThread() throws JMSException {
        sendReceiveMessageMultiThread(MessageType.StreamMessage);

        Assert.assertEquals(producer.double1, ((StreamMessageImpl)receivedMsg).readDouble(), 0);
        Assert.assertEquals(producer.double2, ((StreamMessageImpl)receivedMsg).readDouble(), 0);
    }

    @Test
    public void TestSendQueueObjectMessagge_MultiThread() throws JMSException {
        sendReceiveMessageMultiThread(MessageType.ObjectMessage);

        Assert.assertEquals(producer.testObject.value, ((TestObject)((ObjectMessageImpl)receivedMsg).getObject()).value, 0);
        Assert.assertEquals(producer.testObject.name, ((TestObject)((ObjectMessageImpl)receivedMsg).getObject()).name);
        Assert.assertEquals(producer.testObject.character, ((TestObject) ((ObjectMessageImpl)receivedMsg).getObject()).character);
    }

    private void sendReceiveMessageMultiThread(final MessageType msgType) {
        ExecutorService producerService = Executors.newSingleThreadExecutor();
        final int timeout = 500;

        try {
            prepareSendingMessage(timeout);

            // 发送新消息
            producerService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        synchronized(messageListener.getLock()) {
                            while (messageListener.getStep() != StepEnum.QueueEmpty) {
                                messageListener.getLock().wait();
                            }

                            sendMessage(msgType, "queue-producer-context.xml");
                            messageListener.setStep(StepEnum.SendMessage);
                            messageListener.getLock().notifyAll();
                        }
                    } catch (InterruptedException ex) {
                        System.out.println("Error in sending message: " + ex.getMessage());
                        Assert.fail();
                    }
                }
            });

            // 等待新消息到达
            receivedMsg = receiveAMessage(timeout, new Runnable() {
                @Override
                public void run() {
                    synchronized(messageListener.getLock()) {
                        while (messageListener.getStep() != StepEnum.SendMessage) {
                            try {
                                messageListener.getLock().wait();
                            } catch (InterruptedException ex) {
                                return;
                            }
                        }

                        messageListener.setStep(StepEnum.ReadyPass);
                        messageListener.getLock().notifyAll();
                    }
                }
            });
        }catch (InterruptedException ex) {
            System.out.println(ex.getMessage() + "\nStack: " + ex.getStackTrace());
            Assert.fail();
        } finally {
            producerService.shutdown();
        }
    }

    private Message receiveAMessage (int timeout, Runnable receivingTrigger) throws InterruptedException {
        synchronized (messageListener.getLock()) {
            if (receivingTrigger != null) {
                receivingTrigger.run();
            }

            long start;
            while(messageListener.getMessage() == null) {
                start = System.currentTimeMillis();
                messageListener.getLock().wait(timeout);
                long end = System.currentTimeMillis();
                if (end - start >= timeout) {
                    messageListener.setStep(StepEnum.QueueEmpty);
                    return null;
                }
            }

            Message result = messageListener.getMessage();
            messageListener.setMessage(null);

            return result;
        }
    }

    private void sendMessage(MessageType messageType, String producerContextFileName) {
        ApplicationContext ctx =
                new ClassPathXmlApplicationContext(producerContextFileName);
        JmsProducer jmsProducer = (JmsProducer)ctx.getBean("jmsProducer");
        producer = jmsProducer;
        try {
            switch (messageType) {
                case TextMessage:
                    jmsProducer.sendTextMessage();
                    break;
                case BytesMessage:
                    jmsProducer.sendBytesMessage();
                    break;
                case StreamMessage:
                    jmsProducer.sendStreamMessage();
                    break;
                case MapMessage:
                    jmsProducer.sendMapMessage();
                    break;
                case ObjectMessage:
                    jmsProducer.sendObjectMessage();
                    break;
                default:
                    break;

            }

        } catch (Exception ex) {
            System.out.print(ex.getMessage() + "\nStack: " + ex.getStackTrace());
        }
    }

    private void prepareSendingMessage(int timeout)
            throws InterruptedException {
        synchronized (preparationLock) {
            messageListener = (JmsConsumerAsync)ctx.getBean("queueMessageListener");
            messageListener.setStep(StepEnum.Start);

            while (receiveAMessage(timeout, null) != null) {
            }

            preparationLock.notifyAll();
        }
    }

    private enum MessageType {
        TextMessage,
        BytesMessage,
        MapMessage,
        StreamMessage,
        ObjectMessage
    }
}
