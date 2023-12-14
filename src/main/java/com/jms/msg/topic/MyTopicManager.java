package com.jms.msg.topic;

import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;

public class MyTopicManager {

	protected Map<String, MyTopic> topicMap = new HashMap<String, MyTopic>();

	public void manageTopic(MyTopic myTopic) {
		topicMap.put(myTopic.getCalculateName(), myTopic);
	}

	public void close() {
		topicMap.values().forEach(myTopic -> {
			try {
				myTopic.close();
			} catch (JMSException exp) {
				// we can ignore as of now
			}
		});
	}

	public void sendToTopic(CalculationWork calculationWork) throws Exception {
		MyTopic myTopic = findTopic(calculationWork);
		if (myTopic != null) {
			myTopic.sendCalculationWorkToTopic(calculationWork);
		}
	}

	public MyTopic findTopic(CalculationWork calculationWork) {
		return topicMap.get(calculationWork.getCalculateName());
	}
	
}