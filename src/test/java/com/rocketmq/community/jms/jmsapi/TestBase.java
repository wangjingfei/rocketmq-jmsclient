package com.rocketmq.community.jms.jmsapi;

import com.rocketmq.community.jms.MQConnectionFactory;
import com.rocketmq.community.jms.MQQueue;
import com.rocketmq.community.jms.MQTopic;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.File;
import java.lang.reflect.Array;

public abstract class TestBase extends junit.framework.TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(TestBase.class);



    public TestBase() {
    }



    protected void assertTextMessagesEqual(Message[] firstSet, Message[] secondSet) throws JMSException {
        assertTextMessagesEqual("", firstSet, secondSet);
    }

    protected void assertTextMessagesEqual(String messsage, Message[] firstSet, Message[] secondSet) throws JMSException {
        Assert.assertEquals("Message count does not match: " + messsage, firstSet.length, secondSet.length);

        for (int i = 0; i < secondSet.length; i++) {
            TextMessage m1 = (TextMessage)firstSet[i];
            TextMessage m2 = (TextMessage)secondSet[i];
            assertTextMessageEqual("Message " + (i + 1) + " did not match : ", m1, m2);
        }
    }

    protected void assertEquals(TextMessage m1, TextMessage m2) throws JMSException {
        assertEquals("", m1, m2);
    }

    protected void assertTextMessageEqual(String message, TextMessage m1, TextMessage m2) throws JMSException {
        Assert.assertFalse(message + ": expected {" + m1 + "}, but was {" + m2 + "}", m1 == null ^ m2 == null);

        if (m1 == null) {
            return;
        }

        Assert.assertEquals(message, m1.getText(), m2.getText());
    }

    protected void assertEquals(Message m1, Message m2) throws JMSException {
        assertEquals("", m1, m2);
    }

    protected void assertEquals(String message, Message m1, Message m2) throws JMSException {
        Assert.assertFalse(message + ": expected {" + m1 + "}, but was {" + m2 + "}", m1 == null ^ m2 == null);

        if (m1 == null) {
            return;
        }

        Assert.assertTrue(message + ": expected {" + m1 + "}, but was {" + m2 + "}", m1.getClass() == m2.getClass());

        if (m1 instanceof TextMessage) {
            assertTextMessageEqual(message, (TextMessage)m1, (TextMessage)m2);
        } else {
            assertEquals(message, m1, m2);
        }
    }

    protected void assertBaseDirectoryContainsSpaces() {
        Assert.assertFalse("Base directory cannot contain spaces.", new File(System.getProperty("basedir", ".")).getAbsoluteFile().toString().contains(" "));
    }

    protected void assertArrayEqual(String message, Object[] expected, Object[] actual) {
        Assert.assertEquals(message + ". Array length", expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(message + ". element: " + i, expected[i], actual[i]);
        }
    }

    protected void assertPrimitiveArrayEqual(String message, Object expected, Object actual) {
        int length = Array.getLength(expected);
        Assert.assertEquals(message + ". Array length", length, Array.getLength(actual));
        for (int i = 0; i < length; i++) {
            Assert.assertEquals(message + ". element: " + i, Array.get(expected, i), Array.get(actual, i));
        }
    }
}
