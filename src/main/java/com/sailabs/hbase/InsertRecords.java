package com.sailabs.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertRecords {

	public static int MIN_USER_COUNT = 5;
	public static int MAX_USER_COUNT = 40;

	public static int MIN_PURCHASE_COUNT = 1;
	public static int MAX_PURCHASE_COUNT = 30;

	public void generateRecords() {
		// get how many users you want to add
		// get how many product user going to buy

		MockData.getMockData();

		int userAvailable = ThreadLocalRandom.current().nextInt(MIN_USER_COUNT, MAX_USER_COUNT + 1);

		System.out.println("How many user " + userAvailable);

		for (int i = 0; i < userAvailable; i++) { // lets say there are 6 user
			int productsBuy = ThreadLocalRandom.current().nextInt(MIN_PURCHASE_COUNT, MAX_PURCHASE_COUNT + 1);
			System.out.println("How many Products " + productsBuy);
			String currentUser = MockData.userList
					.get(ThreadLocalRandom.current().nextInt(1, MockData.userList.size() + 1));

			for (int j = 0; j < productsBuy; j++) { // he gonna buy 23 products

				String currentProduct = MockData.productList
						.get(ThreadLocalRandom.current().nextInt(1, MockData.productList.size() + 1));

				System.out.println(currentUser + "," + currentProduct);
			}

		}

	}

	HashMap<String, String> tripHashMap = new HashMap<>();

	// key is phoneNumber , reamingInfo
	HashMap<String, String> userHashMap = new HashMap<>();

	public void generateTripRecords() {
		// get how many users you want to add
		// get how many product user going to buy

		MockData.getTripMockData();

		int userAvailable = ThreadLocalRandom.current().nextInt(MIN_USER_COUNT, MAX_USER_COUNT + 1);

		System.out.println("How many user " + userAvailable);

		for (int i = 0; i < userAvailable; i++) { // lets say there are 6 user
			int tripCount = ThreadLocalRandom.current().nextInt(MIN_PURCHASE_COUNT, MAX_PURCHASE_COUNT + 1);
			System.out.println("How many Trips per user " + tripCount);
			String currentUser = MockData.cabUserList
					.get(ThreadLocalRandom.current().nextInt(1, MockData.cabUserList.size() + 1));

			String[] data = currentUser.split(",");

			userHashMap.put(data[6], currentUser);

			for (int j = 0; j < tripCount; j++) { // he gonna have Y trips

				String tripBelongs = MockData.tripList
						.get(ThreadLocalRandom.current().nextInt(1, MockData.tripList.size() + 1));

				System.out.println(currentUser + "," + tripBelongs);
			}

		}

	}

	public void InsertTripRecord() {

		System.out.println("Create table class Started");
		Configuration conf = HBaseConfig.getHBaseConfig();

		System.out.println("Configuration Object Created ");

		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			System.out.println(connection.isClosed() ? "Connection closed " : " connection Open");
			table = connection.getTable(TableName.valueOf("gobi_trip_table"));
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		if (null != table) {

			Iterator it = userHashMap.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				System.out.println(pair.getKey() + " = " + pair.getValue());

				Put put1 = new Put(Bytes.toBytes(pair.getKey().toString()));
				put1.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("userData"),
						Bytes.toBytes(pair.getValue().toString()));
				// put1.addColumn(Bytes.toBytes("tripInfo"),
				// Bytes.toBytes("qual1"), Bytes.toBytes("val1"));

				it.remove(); // avoids a ConcurrentModificationException
				
				try {
					table.put(put1);
				} catch (IOException e) {
					System.out.print("Exception in putting HBase data : Exception is " + e.toString() );
				}
			}

		}

		
		
	/*	Admin admin = null;
		try {
			admin = connection.getAdmin();
		} catch (MasterNotRunningException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}*/

	}

	// Code to create table if not exists and insert records

}
