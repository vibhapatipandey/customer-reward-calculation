package com.jms.msg.topic;

import java.math.BigInteger;

import javax.jms.Message;
import javax.jms.ObjectMessage;

public class FactorialTopic extends MyTopic {

	public FactorialTopic(String calculateName, String queueName) throws Exception {
		super(calculateName, queueName);
	}

	@Override
	public String processCalculationWorkFromTopic(Message message, String consumerTaskName) throws Exception {
		String answer = "NA";
		if (message != null) {
			ObjectMessage objectMessage = (ObjectMessage) message;
			CalculationWork calculationWork = (CalculationWork) objectMessage.getObject();
			if (calculationWork != null) {
				String value = calculationWork.getValue();
				try {
					long longValue = Long.parseLong(value);
					BigInteger factorialValue = BigInteger.valueOf(1);
					for (long i = 1; i <= longValue; i++) {
						factorialValue = factorialValue.multiply(BigInteger.valueOf(i));
					}
					answer = factorialValue.toString();
				} catch (NumberFormatException exp) {
					System.out.println("Can't calculate factorial of " + value);
				}
			}
			message.acknowledge();
		}
		return answer;
	}

}