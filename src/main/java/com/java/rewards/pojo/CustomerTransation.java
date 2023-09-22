package com.java.rewards.pojo;

import java.time.LocalDate;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class CustomerTransation {
	
	private String customerName;
	private double transactionAmount;
	private LocalDate transactionDate;
	
	
	
	public int calculateRewardPoints() {
		double pointOver100 =Math.max(0, transactionAmount-100)*2;
		double pointOver50 = Math.max(0, transactionAmount-50);
		
		return (int)(pointOver100+pointOver50);
	}
	
}
	

