package com.jms.msg.topic;

import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static org.apache.activemq.ActiveMQConnection.DEFAULT_BROKER_URL;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

public abstract class MyTopic {
	
	protected String calculateName;
	protected String topicName;
	protected ConnectionFactory connectionFactory;
	protected Connection connection;
	protected Session session;
	protected Destination destination;
	protected MessageProducer producer;
	protected MessageConsumer consumer;
	protected int noOfSend = 0;
	
	public MyTopic(String calculateName, String queueName) throws Exception {
		super();
		this.calculateName = calculateName;
		// The name of the queue.
		this.topicName = queueName;
		// DEFAULT_BROKER_URL is : tcp://localhost:61616 
		connectionFactory = new ActiveMQConnectionFactory(DEFAULT_BROKER_URL);
		connection = connectionFactory.createConnection("admin", "admin");
		connection.start();
		// Creating a non-transactional session to send/receive JMS message.
		session = connection.createSession(false, AUTO_ACKNOWLEDGE);
		destination = session.createTopic(this.topicName);
		// MessageProducer is used for sending (producing) messages to the queue.
		producer = session.createProducer(destination);
		// MessageConsumer is used for receiving (consuming) messages from the queue.
		consumer = session.createConsumer(destination);
		consumer.setMessageListener(new MyTopicListener(this, queueName + "Consumer"));
	}

	public void close() throws JMSException {
		producer.close();
		producer = null;
		consumer.close();
		session.close();
		session = null;
		connection.close();
		connection = null;
	}
	
	public void sendCalculationWorkToTopic(CalculationWork calculationWork) throws Exception {
		System.out.printf("%40s | %10s | %-50s\n", topicName + "Producer", "Sending", calculationWork);
		ObjectMessage message = session.createObjectMessage(calculationWork);
		// push the message into queue
		producer.send(message);
		noOfSend++;
	}

	public String getCalculateName() {
		return calculateName;
	}

	public String getTopicName() {
		return topicName;
	}

	public Connection getConnection() {
		return connection;
	}

	public Session getSession() {
		return session;
	}

	public Destination getDestination() {
		return destination;
	}

	public MessageProducer getProducer() {
		return producer;
	}
	
	public abstract String processCalculationWorkFromTopic(Message message, String consumerTaskName) throws Exception;

	private static class MyTopicListener implements MessageListener {

		private MyTopic topic;
		private String consumerTaskName;

		public MyTopicListener(final MyTopic queue, String consumerTaskName) {
			super();
			this.topic = queue;
			this.consumerTaskName = consumerTaskName;
		}

		@Override
		public void onMessage(Message message) {
			// on poll the message from the queue
			try {
				ObjectMessage objectMessage = (ObjectMessage) message;
				CalculationWork calculationWork = (CalculationWork) objectMessage.getObject();
				String answer = topic.processCalculationWorkFromTopic(message, consumerTaskName);
				System.out.printf("%40s | %10s | %-50s \n", consumerTaskName, topic.getCalculateName(), calculationWork + " : " +answer);
			} catch (Exception exp) {
				// we can ignore as of now
			}

		}

	}
}
