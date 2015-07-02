package com.rocketmq.community.jms.message;

import com.alibaba.rocketmq.common.message.Message;

import javax.jms.JMSException;
import javax.jms.TextMessage;

public class TextMessageImpl extends MessageBase implements TextMessage {
    private String text;

    public TextMessageImpl() {
    }

    public TextMessageImpl(String text, boolean readOnly) {
        this.text = text;
        this.readOnly = readOnly;
    }

    @Override
    public void setText(String string) throws JMSException {
        checkReadOnly();
        text = string;
    }
    
    @Override
    public String getText() throws JMSException {
        return text;
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        text = null;
    }

    @Override
    public Message convert() throws JMSException {
        if (text == null) {
            return null;
        }

        Message message = new Message(this.getJMSDestination().toString(), // topic
            JMS_SOURCE + getTag(), // tag
            text.getBytes()); // body
        message.putUserProperty(MSG_TYPE_NAME, MessageTypeEnum.TextMessage.toString());

        copyProperties(message);
        return message;
    }
}
