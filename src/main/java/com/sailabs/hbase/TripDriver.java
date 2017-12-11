package com.sailabs.hbase;

public class TripDriver {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		createTable();
		insertRecord();
	}

	public static void createTable() {
		CreateTable createTable = new CreateTable();
		createTable.createTripTable();
	}

	public static void insertRecord() {
		InsertRecords insertRecords = new InsertRecords();
		insertRecords.generateTripRecords();
		insertRecords.InsertTripRecord();

	}

}
