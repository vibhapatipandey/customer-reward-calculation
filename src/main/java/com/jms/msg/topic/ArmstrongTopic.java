package com.jms.msg.topic;

import javax.jms.Message;
import javax.jms.ObjectMessage;

public class ArmstrongTopic extends MyTopic {

	public ArmstrongTopic(String calculateName, String topicName) throws Exception {
		super(calculateName, topicName);
	}

	@Override
	public String processCalculationWorkFromTopic(Message message, String consumerTaskName) throws Exception {
		String answer = "false";
		CalculationWork calculationWork = null;
		if (message != null) {
			ObjectMessage objectMessage = (ObjectMessage) message;
			calculationWork = (CalculationWork) objectMessage.getObject();
			if (calculationWork != null) {
				String value = calculationWork.getValue();
				try {
					long longValue = Long.parseLong(value);
					long number = longValue;
					long armstrongValue = 0;
					while (number != 0) {
						long temp = number % 10;
						armstrongValue = armstrongValue + temp * temp * temp;
						number /= 10;
					}
					answer = Boolean.toString(String.valueOf(armstrongValue).equals(value));
				} catch (NumberFormatException exp) {
					System.out.println("Can't calculate armstrong of " + value);
				}
			}
			message.acknowledge();
		}
		return answer;
	}

}