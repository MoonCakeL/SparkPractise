package spark.SparkStreaming.outputAction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;


public class ConnectJDBCPool {

	//静态的Connection队列，懒加载，在调用的时候在new初始化
	private static LinkedList<Connection> connectionsQueue;

	//加载驱动
	static {
		try {
			Class.forName("comm.mysql.jdbc.Driver");
			System.out.println("Connect JDBC sucess");
		} catch (ClassNotFoundException e) {
			//e.printStackTrace()；
			System.out.println("Connect JDBC fail " + e.getMessage());
		}
	}

	/**
	 * 创建连接池，控制多并发连接
	 * @return
	 */
	public synchronized static Connection getConnection(){

		try {
			if(connectionsQueue == null) {
				connectionsQueue = new LinkedList<>();
				for(int i = 0; i < 10; i++) {
					Connection conn = DriverManager.getConnection(
							"jdbc:mysql://192.168.42.27:3306/test",
							"admin",
							"admin");
					connectionsQueue.push(conn);

					System.out.println("连接成功");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connectionsQueue.poll();
	}


	/**
	 * 归还连接
	 * @param connection
	 */
	public static void returnConnection(Connection connection){
		connectionsQueue.push(connection);
	}
}
