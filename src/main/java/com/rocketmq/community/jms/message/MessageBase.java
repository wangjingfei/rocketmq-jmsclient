package com.rocketmq.community.jms.message;

import com.alibaba.rocketmq.common.message.Message;
import com.rocketmq.community.jms.util.JMSExceptionSupport;
import com.rocketmq.community.jms.util.RemotingSerializableEx;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import java.util.*;

public abstract class MessageBase implements javax.jms.Message {
    public static final byte NULL = 0;
    public static final byte BOOLEAN_TYPE = 1;
    public static final byte BYTE_TYPE = 2;
    public static final byte CHAR_TYPE = 3;
    public static final byte SHORT_TYPE = 4;
    public static final byte INTEGER_TYPE = 5;
    public static final byte LONG_TYPE = 6;
    public static final byte DOUBLE_TYPE = 7;
    public static final byte FLOAT_TYPE = 8;
    public static final byte STRING_TYPE = 9;
    public static final byte BYTE_ARRAY_TYPE = 10;
    public static final byte MAP_TYPE = 11;
    public static final byte LIST_TYPE = 12;
    public static final byte BIG_STRING_TYPE = 13;
    protected static final int TYPE_NOT_AVAILABLE = -100;

    private Destination destination;
    protected String tag;
    final static public String JMS_SOURCE = "JMS_SOURCE";
    final static public String MSG_TYPE_NAME = "MSG_TYPE";
    final static public String MSG_TAG_NAME = "MSG_TAG_NAME";

    protected boolean readOnly;
    protected byte[] content;
    protected Map<String, String> properties = new HashMap<String, String>();   // RocketMq的消息只支持这种类型的property.

    protected MessageBase() {
    }

    protected MessageBase(byte[] content, Boolean readOnly) {
        this.content = content;
        this.readOnly = readOnly;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public byte[] getContent() {
        return content;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    @Override
    public String getJMSMessageID() throws JMSException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setJMSMessageID(String id) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return destination;
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        this.destination = destination;
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getJMSType() throws JMSException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clearProperties() throws JMSException {
        properties.clear();
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return properties.containsKey(name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        try {
            return Boolean.parseBoolean(properties.get(name));
        } catch (Exception ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        try {
            return Byte.parseByte(properties.get(name));
        } catch (Exception ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        try {
            return Short.parseShort(properties.get(name));
        } catch (Exception ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        try {
            return Integer.parseInt(properties.get(name));
        } catch (Exception ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        try {
            return Long.parseLong(properties.get(name));
        } catch (Exception ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        try {
            return Float.parseFloat(properties.get(name));
        } catch (Exception ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        try {
            return Double.parseDouble(properties.get(name));
        } catch (Exception ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        try {
            return properties.get(name);
        } catch (Exception ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        try {
            return properties.get(name);
        } catch (Exception ex) {
            throw JMSExceptionSupport.create(ex);
        }
    }

    @Override
    public Enumeration getPropertyNames() throws JMSException {
        return null;
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        properties.put(name, new Boolean(value).toString());
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        properties.put(name, new Byte(value).toString());
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        properties.put(name, new Short(value).toString());
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        properties.put(name, new Integer(value).toString());
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        properties.put(name, new Long(value).toString());
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        properties.put(name, new Float(value).toString());
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        properties.put(name, new Double(value).toString());
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        if (name.equalsIgnoreCase(MSG_TAG_NAME)) {
            tag = value;
        }

        properties.put(name, value);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        properties.put(name, value.toString());
    }

    @Override
    public void acknowledge() throws JMSException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clearBody() throws JMSException {
        readOnly = false;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly() {
        return readOnly;
    }
    public abstract Message convert() throws JMSException ;

    protected void copyProperties(Message message) {
        Iterator iterator = properties.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry)iterator.next();
            String key = JMS_SOURCE + (String)entry.getKey();
            String value = (String)entry.getValue();

            message.putProperty(key, value);
        }
    }

    public enum MessageTypeEnum {
        TextMessage,
        StreamMessage,
        MapMessage,
        ObjectMessage,
        BytesMessage
    }

    protected void checkReadOnly() throws MessageNotWriteableException {
        if (readOnly) {
            throw new MessageNotWriteableException("Message body is read-only");
        }
    }

    public MessageBase copy() {
        String json = RemotingSerializableEx.toJsonWithClass(this);
        return RemotingSerializableEx.fromJson(json, this.getClass());
    }
}

