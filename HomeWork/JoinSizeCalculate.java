
import java.sql.Connection;
import java.sql.Statement;
import java.sql.*;

public class JoinSizeCalculate {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java JoinSizeCalculator <DB_URL> <Table1> <Table2>");
            System.out.println("Please follow the above format argument");
            return;
        }

        String dbUrl = args[0];
        String table1 = args[1];
        String table2 = args[2];

//        String table1 = "student";
//        String table2 = "course";



//        Dbconnect db=new Dbconnect();
//        String url="jdbc:postgresql://localhost:5432/testdb?user=postgres&password=root";


        try {
//            Connection conn=db.connect_to_db(dbUrl);
            Connection conn = DriverManager.getConnection(dbUrl);
            Statement stmt = conn.createStatement();

            // Calculate estimated natural join size
            int estimatedSize = estimatedJoinSize(conn, table1, table2);
            System.out.println("Estimated e the size of the natural join : " + estimatedSize);

            // Calculate actual join size
            int actualSize = actualJoinSize(conn, table1, table2);
            System.out.println(" The size of actual natural join: " + actualSize);

            // Calculate estimation error
            int estimationError = estimatedSize - actualSize;
            System.out.println("Estimation Error: " + estimationError);
            if(estimationError>=0)
            {
                System.out.println("over-estimate");
            }
            else {
                System.out.println("under-estimate");
            }

            conn.close();
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        }

    }

    /**
     * Estimated Join Size
     * @param connection Database connection
     * @param table1     First table name
     * @param table2     Second table name
     * @return           Estimated join size
     * @throws SQLException
     */
    private static int estimatedJoinSize(Connection connection, String table1, String table2) throws SQLException {
        Statement stmt = connection.createStatement();

        ResultSet rs1 = stmt.executeQuery("SELECT COUNT(*) FROM " + table1);
        rs1.next();
        int count1 = rs1.getInt(1);

        ResultSet rs2 = stmt.executeQuery("SELECT COUNT(*) FROM " + table2);
        rs2.next();
        int count2 = rs2.getInt(1);

        stmt.close();

        return count1 * count2;
    }

    /**
     * Calculates the actual natural join size of two tables.
     *
     * @param connection Database connection
     * @param table1     First table name
     * @param table2     Second table name
     * @return           Actual join size
     * @throws SQLException
     */
    private static int actualJoinSize(Connection connection, String table1, String table2) throws SQLException {
        Statement stmt = connection.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table1 + " NATURAL JOIN " + table2);
        rs.next();
        int count = rs.getInt(1);

        stmt.close();

        return count;
    }
}
