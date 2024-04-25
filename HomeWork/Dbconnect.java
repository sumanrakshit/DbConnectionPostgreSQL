import java.sql.Connection;
import java.sql.DriverManager;

public class Dbconnect {
    public Connection connect_to_db(String url){
        Connection conn=null;
        try{
//            Class.forName("org.postgresql.Driver");
            conn= DriverManager.getConnection(url);
            if(conn!=null){
                System.out.println("Connection Established");
            }
            else{
                System.out.println("Connection Failed");
            }

        }catch (Exception e){
            System.out.println(e);
        }
        return conn;
    }

}
