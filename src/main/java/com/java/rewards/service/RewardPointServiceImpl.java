package com.java.rewards.service;

import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.java.rewards.pojo.CustomerTransation;

@Component
public class RewardPointServiceImpl implements RewardPointService {

	private static Map<String, List<CustomerTransation>> customerRewardsData = new HashMap<>(); 

	@Override
	public int addCustomerRewards(List<CustomerTransation> customerList) {
		var count =0;
		for (CustomerTransation customerTransation : customerList) {
			var customerName = customerTransation.getCustomerName();
			customerRewardsData.computeIfAbsent(customerName, newlist -> new ArrayList<>()).add(customerTransation);
			count++;
		}
		return count;
	}

	@Override
	public Map<String, Map<Month, Integer>> fetchRewardPoints() {
		createTestDate();
		Map<String,Map<Month,Integer>> rewardPoins = new HashMap<>();
		customerRewardsData.forEach((customerName,rewardPoint) ->{
			Map<Month,Integer> monthlypoints = rewardPoint.stream()
					.collect(Collectors.groupingBy(point -> point.getTransactionDate().getMonth(),
							Collectors.summingInt(CustomerTransation::calculateRewardPoints)));
			rewardPoins.put(customerName, monthlypoints);
		});	
		return rewardPoins;
	}
	
	private void createTestDate() {
		List<CustomerTransation> customerList= Arrays.asList(
				new CustomerTransation("Customer1",120.0,LocalDate.of(2023, 9, 15)),
				new CustomerTransation("Customer2",80.0,LocalDate.of(2023, 7, 05)),
				new CustomerTransation("Customer3",170.0,LocalDate.of(2023, 6, 12)),
				new CustomerTransation("Customer4",40.0,LocalDate.of(2023, 8, 15)));
		customerRewardsData = customerList.stream().collect(Collectors.groupingBy(CustomerTransation::getCustomerName));
	
	}

}
