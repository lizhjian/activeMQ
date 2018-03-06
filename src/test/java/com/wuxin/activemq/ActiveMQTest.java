package com.wuxin.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

import javax.jms.*;

public class ActiveMQTest {
     //编写发送方 生产者
    @Test
    public void  test1() throws Exception{
        //1.连接工厂
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        //2.连接工厂对象
        Connection  connection = connectionFactory.createConnection();
        //3.连接MQ对象
        connection.start();
        //4.获取session对象 下不支持事物 + 自动应答
        Session session =  connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
       //5.通过session对象创建topic
       Topic topic =  session.createTopic("ip:wuxinTopic");
       //6.通过session来创建消息的发送者
       MessageProducer messageProducer =  session.createProducer(topic);
       //7.通过session创建消息对象
       TextMessage textMessage = session.createTextMessage("ping11");
       //8.发送消息
       messageProducer.send(textMessage);
       //9.关闭资源
        messageProducer.close();
        session.close();
        connection.close();
    }

    //消费者
    @Test
    public void  test2() throws Exception{
        //1.连接工厂
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        //2.连接工厂对象
        Connection  connection = connectionFactory.createConnection();
        //3.连接MQ对象
        connection.start();
        //4.获取session对象 下不支持事物 + 自动应答
        final Session session =  connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        //5通过session对象创建topic
        Topic topic =  session.createTopic("ip:wuxinTopic");
        //6.通过session来创建消息的消费者
        MessageConsumer messageConsumer =  session.createConsumer(topic);
        //7.指定消息监听器
        messageConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                //当监听的topic
                TextMessage textMessage =(TextMessage)message;
                try {
                    if(textMessage.getText().equals("ping")){
                        System.out.println("消费者接收到了消息"+textMessage.getText());
                        //客户端手动应答
                        message.acknowledge();
                    }else{
                        //模拟消息失败
                        System.out.println("消息处理失败");
                        //失败后通知mq重发
                        session.recover();
                        int i = 1/0;
                    }

                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
        while (true){

        }
    }
}
