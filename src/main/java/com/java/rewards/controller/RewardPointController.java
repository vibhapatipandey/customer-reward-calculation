package com.java.rewards.controller;

import java.time.Month;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.java.rewards.pojo.CustomerTransation;
import com.java.rewards.service.RewardPointService;
import com.java.rewards.service.RewardPointServiceImpl;

@RestController
@RequestMapping("/api")
public class RewardPointController {
	
	@Autowired
	RewardPointService rewardPointService;
		
	 
	
	//method added for create data.
	@PostMapping("/addDetail")
	public ResponseEntity<String> createData(@RequestBody List<CustomerTransation> customerList){
		customerList =Optional.ofNullable(customerList).orElseThrow(()-> new RuntimeException("CustomerTransation should not be null."));
		var count = rewardPointService.addCustomerRewards(customerList);
		
		return ResponseEntity.status(HttpStatus.CREATED).body(count + " Customer details created succesfully");
	}
	
	
	
	// Retrieve data 
	@GetMapping("/reward-points")
	public ResponseEntity<Map<String,Map<Month,Integer>>> getRewardPoint(){
		var rewardPoints = rewardPointService.fetchRewardPoints();
		return ResponseEntity.ok(rewardPoints);
	}
	
	
	

}
