package com.jms.msg.topic;

import java.io.BufferedReader;
import java.io.FileReader;

public class Main {

	private static final String TOPIC_ARMSTRONG = "ArmstrongCalculationTopic";
	private static final String TOPIC_FACTORIAL = "FactorialCalculationTopic";
	private static final String TOPIC_PALINDROME = "PalindromeCalculationTopic";
	
	public static void main(String[] args) throws Exception {
		MyTopicManager topicManager = new MyTopicManager();
		topicManager.manageTopic(new ArmstrongTopic("Armstrong", TOPIC_ARMSTRONG));
		topicManager.manageTopic(new FactorialTopic("Factorial", TOPIC_FACTORIAL));
		topicManager.manageTopic(new PalindromeQueue("Palindrome", TOPIC_PALINDROME));
		
		System.out.printf("%40s | %10s | %-50s\n", "Source", "Action", "Result/Details");
		System.out.println(
				"=================================================================================================================");
		BufferedReader reader = new BufferedReader(new FileReader("./CalculationWork.txt"));
		String line;
        while ((line = reader.readLine()) != null) {
        	CalculationWork calculationWork = CalculationWork.valueOf(line);
        	if (calculationWork != null) {
        		topicManager.sendToTopic(calculationWork);
        	}
        }
        reader.close();
		// just to give graceful time to finish the processing
		Thread.sleep(2000);
		topicManager.close();
		System.out.println(
				"=================================================================================================================");
	}

}
