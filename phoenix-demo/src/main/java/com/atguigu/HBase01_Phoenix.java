package com.atguigu;

import org.apache.phoenix.queryserver.client.ThinClientUtil;

import java.sql.*;

public class HBase01_Phoenix{

    public static void main(String[] args)throws SQLException {

        //1.获取连接地址
        String connectionUrl = ThinClientUtil.getConnectionUrl("hadoop102", 8765);

        //2.创建连接
        Connection connection = DriverManager.getConnection(connectionUrl);

        //3.预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT id,name,addr FROM test1");

        //4.执行查询
        ResultSet resultSet = preparedStatement.executeQuery();

        //5.解析resultSet
        while (resultSet.next()) {
            System.out.println("ID:" + resultSet.getString(1) +
                    ",NAME:" + resultSet.getString(2) +
                    ",ADDR:" + resultSet.getString(3));
        }

        //6.释放资源
        resultSet.close();
        preparedStatement.close();
        connection.close();
    }
}
