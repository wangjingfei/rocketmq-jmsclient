package com.rocketmq.community.jms.helper;

import javax.jms.*;

public class JmsConsumerAsync implements MessageListener {
    private Message message;
    final private Object lock = new Object();
    private StepEnum step = StepEnum.Start;

    public void onMessage(Message message) {
        synchronized (lock) {
            if (step == StepEnum.Start) {
                passMessage(message);
                return;
            }

            while (step != StepEnum.ReadyPass) {
                try {
                    lock.wait();
                } catch (InterruptedException ex) {
                    return;
                }

            }

            passMessage(message);
            setStep(StepEnum.MessagePassed);
        }
    }

    private void passMessage(Message message) {
        this.message = message;
        lock.notifyAll();
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        synchronized (lock) {
            this.message = message;
        }
    }
    public Object getLock() {
        return lock;
    }

    public StepEnum getStep() {
        return step;
    }

    public void setStep(StepEnum step) {
        this.step = step;
    }
}
