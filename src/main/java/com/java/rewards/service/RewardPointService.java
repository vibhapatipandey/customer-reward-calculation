package com.java.rewards.service;

import java.time.Month;
import java.util.List;
import java.util.Map;

import com.java.rewards.pojo.CustomerTransation;

public interface RewardPointService {
	
	int addCustomerRewards(List<CustomerTransation> cust);
	Map<String,Map<Month,Integer>> fetchRewardPoints();

}
