package com.rocketmq.community.jms;

import com.alibaba.rocketmq.client.consumer.*;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.rocketmq.community.jms.message.*;
import com.rocketmq.community.jms.util.JMSExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class MQMessageConsumer implements MessageConsumer {
    private static final Logger log = LoggerFactory.getLogger(MQMessageConsumer.class);

    protected DefaultMQPullConsumer pullConsumer;
    protected DefaultMQPushConsumer pushConsumer;
    protected MessageListener messageListener;
    private MessageQueue[] mqs;
    protected int mqIndex = 0;
    final protected int batchSize = 32;
    protected int consumingMsgIndex = 0;
    protected PullResult pullResult;
    protected String topic;
    protected String tag;
    protected MQSession session;
    protected Boolean started;

    private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();

    MQMessageConsumer(MQSession session, MQConsumer consumer, String topic, String tag) throws JMSException {
        this.session = session;

        if (consumer instanceof DefaultMQPushConsumer) {
            pushConsumer = (DefaultMQPushConsumer)consumer;
        } else {
            pullConsumer = (DefaultMQPullConsumer)consumer;
        }
        this.topic = topic;
        this.tag = tag;
        started = true;
        session.addConsumer(this);
    }

    @Override
    public String getMessageSelector() throws JMSException {
        return tag;
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        return messageListener;
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        final MessageListener tmpListener = listener;
        messageListener = listener;
        if (pushConsumer != null) {
            pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                ConsumeConcurrentlyContext context) {
                    Message message;

                    try {
                        for(MessageExt msg : msgs) {
                            message = convertToJmsMessage(msg);
                            tmpListener.onMessage(message);
                        }
                    } catch (JMSException ex) {
                        log.error("Can't handler message " + msgs.toString() + "\nException: " + ex);
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }

            });
        }
    }

    @Override
    public Message receive() throws JMSException {
        final MessageQueue mq = getMq();
        return receiveInternal(new Callable<PullResult>() {
            @Override
            public PullResult call() throws Exception {
                return pullConsumer.pullBlockIfNotFound(mq, MessageBase.JMS_SOURCE + tag, getMessageQueueOffset(mq), batchSize);
            }
        }, mq, 0);
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        final MessageQueue mq = getMq();
        return receiveInternal(new Callable<PullResult>() {
            @Override
            public PullResult call() throws Exception {
                return pullConsumer.pull(mq, MessageBase.JMS_SOURCE + tag, getMessageQueueOffset(mq), batchSize);
            }
        }, mq, timeout);
    }

    private MessageQueue getMq() throws JMSException {
        if (mqs == null || mqs.length == 0) {
            try {
                mqs = new MessageQueue[1];
                mqs = pullConsumer.fetchSubscribeMessageQueues(topic).toArray(mqs);
            } catch (MQClientException ex) {
                throw JMSExceptionSupport.create(ex);
            }
        }

        MessageQueue mq = mqs[mqIndex];
        System.out.println("Consume from the queue: " + mq);

        return mq;
    }

    private Message receiveInternal(Callable<PullResult> pull, MessageQueue mq, long timeout) throws JMSException {
        Message result = null;

        try {
            pullConsumer.setConsumerPullTimeoutMillis(timeout);
            if (consumingMsgIndex == 0) {
                // 所有之前批量拉取的消息已经处理完了，重新拉取。
                pullResult = pull.call();
            }

            if (pullResult != null) {
                putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        result = convertToJmsMessage(pullResult.getMsgFoundList().get(consumingMsgIndex));
                        consumingMsgIndex++;
                        consumingMsgIndex = consumingMsgIndex < pullResult.getMsgFoundList().size() ? consumingMsgIndex : 0;
                        if (consumingMsgIndex == 0) {
                            // 上一个mq的信息处理完了
                            mqIndex++;
                            mqIndex %= mqs.length;
                        }
                        break;
                    case NO_MATCHED_MSG:
                        break;
                    case NO_NEW_MSG:
                        break;
                    case OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    private Message convertToJmsMessage(MessageExt rawMessage) throws JMSException {
        String msgType = rawMessage.getProperty(MessageBase.MSG_TYPE_NAME);
        if (msgType == null || rawMessage.getBody() == null) {
            return null;
        }

        Message message = null;
        if (msgType.equalsIgnoreCase(MessageBase.MessageTypeEnum.TextMessage.toString())) {
            String content = new String(rawMessage.getBody());
            message = new TextMessageImpl(content, true);
        } else if (msgType.equalsIgnoreCase(MessageBase.MessageTypeEnum.MapMessage.toString())) {
            message = new MapMessageImpl(rawMessage.getBody(), true);
        } else if (msgType.equalsIgnoreCase(MessageBase.MessageTypeEnum.BytesMessage.toString())) {
            if (rawMessage.getBody() != null) {
                message = new BytesMessageImpl(rawMessage.getBody(), true);
            }
        } else if (msgType.equalsIgnoreCase(MessageBase.MessageTypeEnum.StreamMessage.toString())) {
            if (rawMessage.getBody() != null) {
                message = new StreamMessageImpl(rawMessage.getBody(), true);
            }
        } else if (msgType.equalsIgnoreCase(MessageBase.MessageTypeEnum.ObjectMessage.toString())) {
            if (rawMessage.getBody() != null) {
                message = new ObjectMessageImpl(rawMessage.getBody(), true);
            }
        }

        if (rawMessage.getProperties() != null) {
            Iterator iterator = rawMessage.getProperties().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry entry = (Map.Entry)iterator.next();
                String key = (String)entry.getKey();

                if (key.startsWith(MessageBase.JMS_SOURCE)) {
                    key = key.replaceFirst(MessageBase.JMS_SOURCE, "");
                    String value = (String)entry.getValue();
                    message.setStringProperty(key, value);
                }
            }
        }
        return message;
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() throws JMSException {
        if (pullConsumer != null) {
            pullConsumer.shutdown();
        }

        if (pushConsumer != null) {
            pushConsumer.shutdown();
        }
    }


    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        offseTable.put(mq, offset);
    }


    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = offseTable.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }

    public void start() throws JMSException {
        try {
            if (pullConsumer != null) {
                pullConsumer.start();
                log.info("Rocket pull consumer started");
                return;
            }

            if (pushConsumer != null) {
                pushConsumer.start();
                log.info("Rocket push consumer started");
                return;
            }

            log.warn("Rocket consumer unavailable");
        } catch (MQClientException ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }
}
