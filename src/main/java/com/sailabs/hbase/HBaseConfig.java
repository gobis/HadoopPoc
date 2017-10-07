package com.sailabs.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseConfig {
	
	/*
	 * Put your hbase-site.xml file in the conf folder and it SHOULD get picked up automatically
	 * If your hbase-site.xml does not get picked up, then provide the zookeeper locations
	 */
	public static Configuration getHBaseConfig() {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "nn01.itversity.com,nn02.itversity.com,rm01.itversity.com");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("zookeeper.znode.parent", "/hbase-unsecure");
		config.set("hbase.cluster.distributed", "true");
		
		return config;
	}

}
