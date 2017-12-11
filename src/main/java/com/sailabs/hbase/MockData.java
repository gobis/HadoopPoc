package com.sailabs.hbase;

import java.util.ArrayList;

public class MockData {

	public static ArrayList<String> userList = new ArrayList<>();
	public static ArrayList<String> productList = new ArrayList<>();

	public static void main(String[] args) {
		Myres resObj = new Myres();
		resObj.getTripMockData();
		resObj.getMockData();
	}
	
	
			

}
