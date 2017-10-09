package com.sailabs.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class CreateTable {

	public static void main(String[] args) {
		System.out.println("Create table class Started" );
		Configuration conf = HBaseConfig.getHBaseConfig();
		
		System.out.println("Configuration Object Created " );
		
		Connection connection = null;
		 try {
			 connection = ConnectionFactory.createConnection(conf);
			 System.out.println(connection.isClosed() ? "Connection closed " : " connection Open");
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		Admin admin = null;
		try {
			admin = connection.getAdmin();
		}  catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		System.out.println(null == admin ? "Admin not created":"admin created" );
		
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("my-table"));
		tableDescriptor.addFamily(new HColumnDescriptor("colfam1"));
		tableDescriptor.addFamily(new HColumnDescriptor("colfam2"));
		tableDescriptor.addFamily(new HColumnDescriptor("colfam3"));
		
		System.out.println("table constructed" );
		
		/*try {
			admin.createTable(tableDescriptor);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		System.out.println("executed table creation code" );
		boolean tableAvailable = false;
		try {
			tableAvailable = admin.isTableAvailable(TableName.valueOf("gobi_test_table"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("tableAvailable = " + tableAvailable);

	}
	
	
	public boolean createTripTable(){
		boolean hasTable = false; 
		
		System.out.println("Create table class Started" );
		Configuration conf = HBaseConfig.getHBaseConfig();
		
		System.out.println("Configuration Object Created " );
		
		Connection connection = null;
		 try {
			 connection = ConnectionFactory.createConnection(conf);
			 System.out.println(connection.isClosed() ? "Connection closed " : " connection Open");
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
		
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("gobi_trip_table"));
		tableDescriptor.addFamily(new HColumnDescriptor("userInfo"));
		tableDescriptor.addFamily(new HColumnDescriptor("tripInfo"));
		try {
			admin.createTable(tableDescriptor);
			System.out.println("table created" );
			hasTable = true ;
		}catch(TableExistsException e){
			System.out.println("table already available" );
			hasTable = true ;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			hasTable = false ;
		} 
	
		try {
			connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return hasTable;
	}
	
	
	public boolean createProductTable(){
	boolean hasTable = false; 
		
		System.out.println("Create table class Started" );
		Configuration conf = HBaseConfig.getHBaseConfig();
		
		System.out.println("Configuration Object Created " );
		
		Connection connection = null;
		 try {
			 connection = ConnectionFactory.createConnection(conf);
			 System.out.println(connection.isClosed() ? "Connection closed " : " connection Open");
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		Admin admin = null;
		try {
			admin = connection.getAdmin();
		}  catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		System.out.println(null == admin ? "Admin not created":"admin created" );
		
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("gobi_product_table"));
		tableDescriptor.addFamily(new HColumnDescriptor("userInfo"));
		tableDescriptor.addFamily(new HColumnDescriptor("productInfo"));
		try {
			admin.createTable(tableDescriptor);
			System.out.println("table created" );
			hasTable = true ;
		}catch(TableExistsException e){
			System.out.println("table already available" );
			hasTable = true ;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			hasTable = false ;
		} 
	
		
		return hasTable;
	}

	
	
	
	
	

}
