package com.jms.msg.test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Test1 {
	
	public static void main(String arg[]) {
		List<Integer> list= Arrays.asList(2,14,7,9,10,5,13);
		Collections.sort(list);
	 System.out.println("value is:"+list.get(list.size()-3));
	}
	
	

}
