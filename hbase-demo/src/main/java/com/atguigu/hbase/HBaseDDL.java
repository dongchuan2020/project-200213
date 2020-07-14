package com.atguigu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDDL {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭方法
     * @throws IOException
     */
    public static void close()throws IOException{
        if(connection != null){
            connection.close();
        }
        if(admin != null){
            admin.close();
        }
    }

    /**
     * 创建命名空间
     * @param namespace
     * @throws IOException
     */
    public static void createNamespace(String namespace)throws IOException{

        NamespaceDescriptor descriptor = NamespaceDescriptor.create(namespace).build();

        try {
            admin.createNamespace(descriptor);
        }catch (NamespaceExistException e){
            System.out.println("命名空间已存在！");
        }

        close();
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName)throws IOException{
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 创建表
     * @param tableName
     * @param cfs
     * @throws IOException
     */
    public static void createTable(String tableName,String... cfs)throws IOException{

        if(cfs.length <= 0){
            System.out.println("请输入列族信息！");
            return;
        }

        if(isTableExist(tableName)){
            System.out.println(tableName + "表已经存在！");
            return;
        }

        //1.表描述器创建者对象
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(
                TableName.valueOf(tableName));

        //2.创建列族描述器
        for (String cf : cfs) {

            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(cf)).build();

            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }

        //3.创建表描述器对象
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        //4.创建表
        admin.createTable(tableDescriptor);
    }

    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(String tableName)throws IOException{
        if(!isTableExist(tableName)){
            System.out.println(tableName + "该表不存在！");
            return;
        }

        TableName tn = TableName.valueOf(tableName);

        //先禁用表
        admin.disableTable(tn);

        //删除表
        admin.deleteTable(tn);
    }

    public static void main(String[] args)throws IOException {

//        boolean flag = isTableExist("student");
//        System.out.println(flag);

//        createTable("stu1","info1","info2");

        dropTable("student");
    }


}
