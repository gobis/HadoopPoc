package com.sailabs.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class ListTables {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		System.out.println("Create table class Started" );
		Configuration conf = HBaseConfig.getHBaseConfig();
		
		System.out.println("Configuration Object Created " );
		
		Connection connection = null;
		 try {
			 connection = ConnectionFactory.createConnection(conf);
			 System.out.println(connection.isClosed() ? "Connection closed " : "connection Open");
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		Admin admin = null;
		try {
			admin = connection.getAdmin();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		System.out.println(null == admin ? "Admin not created":"admin created" );
		
	
		try {
			HTableDescriptor[] tabls = admin.listTables();
			
	        for (HTableDescriptor hTableDescriptor : tabls) {
	        	System.out.println(hTableDescriptor.getTableName().getName().toString());
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		

	}

}
