package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBase03_Coprocessor {

    //声明Connection以及Admin
    private static Connection connection;
    private static Admin admin;

    static {
        //1.创建配置信息,并指定连接的集群
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        //2.创建连接器
        try {
            connection = ConnectionFactory.createConnection(configuration);
            //3.创建DDL操作对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() throws IOException {
        admin.close();
        connection.close();
    }

    public static boolean isTableExist(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public static void createTable(String tableName, String... cfs)throws IOException {
        //1.判断是否有列族信息
        if (cfs.length <= 0) {
            System.out.println("请输入列族信息！！！");
            return;
        }

        //2.判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println(tableName + "该表已存在！");
            return;
        }

        //3.创建表描述器Builder对象
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

        //4.循环放入列族信息
        for (String cf : cfs) {

            //5.创建列族描述器
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build();

            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }

        //6.创建表描述器
        tableDescriptorBuilder.setCoprocessor("com.atguigu.MyCoprocessor");
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        //7.创建表
        admin.createTable(tableDescriptor);
    }

    public static void main(String[] args)throws IOException {

        createTable("stu1","info");

        close();
    }
}
