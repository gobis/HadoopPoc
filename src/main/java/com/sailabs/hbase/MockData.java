package com.sailabs.hbase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class MockData {

	public static ArrayList<String> userList = new ArrayList<>();
	public static ArrayList<String> productList = new ArrayList<>();

	public static void main(String[] args) {
		getMockData();
		getTripMockData();
	}

	public static void getMockData() {

		// read clean_retail_data and user_info also count # of lines
		try {
			BufferedReader reader = new BufferedReader(
					new FileReader("./src/main/java/com/sailabs/mockData/user_info"));
			reader.readLine(); // ignore first line as it has header information
			String line = null;
			while ((line = reader.readLine()) != null) {
				userList.add(line);
			}
			
			reader.close();

			System.out.println(" Number of users  " + userList.size());

			line = null;

			BufferedReader reader2 = new BufferedReader(
					new FileReader("./src/main/java/com/sailabs/mockData/clean_retail_data"));
			reader2.readLine(); // ignore first line as it has header
								// information
			while ((line = reader2.readLine()) != null) {
				productList.add(line);
			}
			reader2.close();

			System.out.println(" Number of Products  " + productList.size());

		} catch (Exception e) {
			System.out.println(" Exception " + e.getMessage());
		}

	}
	
	
	public static ArrayList<String> cabUserList = new ArrayList<>();
	public static ArrayList<String> tripList = new ArrayList<>();
	
	
	public static void getTripMockData() {

		// read clean_retail_data and user_info also count # of lines
		try {
			BufferedReader reader = new BufferedReader(
					new FileReader("./src/main/java/com/sailabs/mockData/cab_user_info.csv"));
			reader.readLine(); // ignore first line as it has header information
			String line = null;
			while ((line = reader.readLine()) != null) {
				cabUserList.add(line);
			}
			
			reader.close();

			System.out.println(" Number of cab users  " + cabUserList.size());

			line = null;

			BufferedReader reader2 = new BufferedReader(
					new FileReader("./src/main/java/com/sailabs/mockData/trip_info.csv"));
			reader2.readLine(); // ignore first line as it has header
								// information
			while ((line = reader2.readLine()) != null) {
				tripList.add(line);
			}
			reader2.close();

			System.out.println(" Number of Trips  " + tripList.size());

		} catch (Exception e) {
			System.out.println(" Exception " + e.getMessage());
		}

	}
	

}
