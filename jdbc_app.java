import java.io.File;  
import java.io.FileNotFoundException;  
import java.util.Scanner; 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

public class jdbc_app {
    static private boolean debug = true;

    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String URL = "jdbc:mysql://localhost:3306/";
    //static final String URL = "jdbc:mysql://127.0.0.1:3306/";

    //  Database credentials
    static final String DBASE = "company";
    static final String USER = "root";
    static final String PASS = "";

    public static void main(String[] args) {
        Scanner console = new Scanner(System.in);
        Connection conn = null;
        PreparedStatement stmt1 = null;
        PreparedStatement stmt2 = null;
        PreparedStatement stmt3 = null;
        String fileprojnumber ="";
        String projectname = "";
        String projectnum = "";
        String department = "";
        String firstName = "";
        String lastName = "";
        String dnum = "";
        int assigmentNum = 0;
        double totalnum = 0;
        String filename;
        String fullname = "";
        double avg;
        int count = 0;
        try {
            //STEP 2: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            //STEP 3: Open a connection
            conn = DriverManager.getConnection(URL + DBASE, USER, PASS);
            //if connection is null it will not connect
            if (conn != null) {
                System.out.println();
                System.out.println("Successfully connected to DB: " + DBASE);
                System.out.println();
                //queries to get results
                String outterquery = "SELECT pname, pnumber, dnum from project WHERE pnumber = ?";
                String innerquery1 = "SELECT dname, fname, lname from department d, employee e WHERE d.mgrssn = e.ssn and dnumber = ?";
                String innerquery2 = "SELECT count(hours) from  works_on where pno = ? ";

                stmt1 = conn.prepareStatement(outterquery);
                stmt2 = conn.prepareStatement(innerquery1);
                stmt3 = conn.prepareStatement(innerquery2);
                System.out.print("Enter a file name: ");

                filename = console.nextLine();
                System.out.println();
                //strings for the headers
                String s1 = "Proj#";
                String s2 = "ProjName";
                String s3 = "Department";
                String s4 = "Manager";
                String s5 = "# of assignments ";


                try {
                    //opens file to get project numbers
                    File myObj = new File(filename);
                    Scanner myReader = new Scanner(myObj);
                    System.out.println();
                    //prints out the headers
                    System.out.format("%5s%32s%32s%32s%32s", s1, s2, s3, s4, s5);
                    System.out.println();
                    System.out.println("--------------------------------------------------------------------------------------------------------------------------------------");
                    while (myReader.hasNextLine()) {
                        fileprojnumber = myReader.nextLine();


                        stmt1.clearParameters();
                        //places project number from file in ?
                        stmt1.setString(1, fileprojnumber);
                        //STEP 4: Execute a query

                        //if (debug) // stmt is a PreparedStatement object and cannot be displayed
                        //  System.out.println("SQL was:\n "+stmt.toString());
                        //Execute outer query
                        ResultSet rs = stmt1.executeQuery();
                        //if the query buffer is empty, it will print out invalid message and move onto next loop
                        if(!rs.isBeforeFirst()) {
                            System.out.println("Invalid input project number: " + fileprojnumber);
                            continue;
                        }
                        else {
                            //STEP 5: Extract data from result set
                            while (rs.next()) {
                                //Retrieve by column name
                                projectname = rs.getString(1);
                                projectnum = rs.getString(2);
                                dnum = rs.getString(3);

                                SQLWarning warning = stmt1.getWarnings();
                                if (warning != null) {
                                    System.out.println("\n---Warning---\n");
                                    while (warning != null) {
                                        System.out.println("Message: " + warning.getMessage());
                                        System.out
                                                .println("SQLState: " + warning.getSQLState());
                                        System.out.print("Vendor error code: ");
                                        System.out.println(warning.getErrorCode());
                                        System.out.println("");
                                        warning = warning.getNextWarning();
                                    }
                                }



                            } }
                        stmt2.clearParameters();
                        //places dnum from outer query in ?
                        stmt2.setString(1, dnum);
                        //Execute inner query
                        ResultSet rs2 = stmt2.executeQuery();
                        while (rs2.next()) {
                            //Retrieve by column name
                            department = rs2.getString(1);
                            firstName = rs2.getString(2);
                            lastName = rs2.getString(3);
                            fullname = firstName + " " + lastName;
                            SQLWarning warning = stmt2.getWarnings();
                            if (warning != null) {
                                System.out.println("\n---Warning---\n");
                                while (warning != null) {
                                    System.out.println("Message: " + warning.getMessage());
                                    System.out
                                            .println("SQLState: " + warning.getSQLState());
                                    System.out.print("Vendor error code: ");
                                    System.out.println(warning.getErrorCode());
                                    System.out.println("");
                                    warning = warning.getNextWarning();
                                }
                            }

                        } rs2.close();

                        stmt3.clearParameters();
                        //places project number from outerquery in ?
                        stmt3.setString(1, projectnum);
                        //Execute inner query
                        ResultSet rs3 = stmt3.executeQuery();


                        while (rs3.next()) {
                            //Retrieve by column name
                            assigmentNum = rs3.getInt(1);
                            totalnum =  totalnum + assigmentNum; //used to calculate average
                            SQLWarning warning = stmt3.getWarnings();
                            count++; //acts as counters for total num for average
                            if (warning != null) {
                                System.out.println("\n---Warning---\n");
                                while (warning != null) {
                                    System.out.println("Message: " + warning.getMessage());
                                    System.out
                                            .println("SQLState: " + warning.getSQLState());
                                    System.out.print("Vendor error code: ");
                                    System.out.println(warning.getErrorCode());
                                    System.out.println("");
                                    warning = warning.getNextWarning();
                                }
                            }

                        } rs3.close();


                        rs.close();
                        //prints out output in formatted form
                        System.out.format("%5s%32s%32s%32s%32d", projectnum, projectname, department, fullname, assigmentNum);
                        System.out.println();


                    }
                    //calacutes the average
                    avg = totalnum/count;
                    System.out.println();
                    //prints out the average
                    System.out.println("The average number of work assignments per valid project is: " + avg);
                    //handles error for file
                }  catch (FileNotFoundException e) {
                    System.out.println("An error occurred: The file does not exist");
                    e.printStackTrace();
                }
            }
        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            //finally block used to close resources
            //STEP 6: Clean-up environment
            try {
                if (stmt1 != null)
                    stmt1.close();
                if (stmt2 != null)
                    stmt2.close();
                if (stmt3 != null)
                    stmt3.close();
            } catch (SQLException se2) {} // nothing we can do
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            } //end finally try
        } //end try

    } //end main
}
