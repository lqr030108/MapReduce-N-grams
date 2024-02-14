import java.sql.Connection;
import java.sql.DriverManager;

public class JDBCUtil {

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String MYSQL_URL =
            "jdbc:mysql://localhost:3306/associative-word";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "liqirong2003";
    /**
     * 获取mysql的连接对象
     * @return
     */
    public static Connection getConnection(){
        Connection conn = null;
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            conn = DriverManager.getConnection(MYSQL_URL,MYSQL_USERNAME,MYSQL_PASSWORD);
        }catch (Exception e){
            e.printStackTrace();
        }
        return conn;
    }
}

