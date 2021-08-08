package sqllite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

class Movie {

    private Connection connect() {
        // SQLite connection string
        String url = "jdbc:sqlite:D://javaProjects/JDBC/hello.db";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    
    public void insert(int movie_id, String movie_name, String actor, String actress,String director,int yr_of_release) {
        String sql = "INSERT INTO movies(movie_id,movie_name,actor,actress,director,yr_of_release) VALUES(?,?,?,?,?,?)";

        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, movie_id);
            pstmt.setString(2, movie_name);
            pstmt.setString(3, actor);
            pstmt.setString(4, actress);
            pstmt.setString(5, director);
            pstmt.setInt(6, yr_of_release);
            
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    public void selectAll(){
        String sql = "SELECT movie_id, movie_name, actor,actress,director,yr_of_release FROM movies";
       
        try (Connection conn = this.connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){
            
           
            while (rs.next()) {
                System.out.println(rs.getInt("movie_id") +  "\t" + 
                                   rs.getString("movie_name") + "\t" +
                                   rs.getString("actor") + "\t" +
                                   rs.getString("actress") + "\t" +
                                   rs.getString("director") + "\t" +
                                   rs.getInt("yr_of_release"));
            }
        }
        
        catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    public void ParameterizedQuery(String actor){
        String sql = "SELECT movie_id, movie_name, actor,actress,director,yr_of_release FROM movies Where actor=?";
 try (Connection conn = this.connect();
      PreparedStatement pstmt  = conn.prepareStatement(sql)){
     
     // set the value
     pstmt.setString(1,actor);
     //
     ResultSet rs  = pstmt.executeQuery();
     
     
     while (rs.next()) {
         System.out.println(rs.getInt("movie_id") +  "\t" + 
                            rs.getString("movie_name") + "\t" +
                            rs.getString("actor") + "\t" +
                            rs.getString("actress") + "\t" +
                            rs.getString("director") + "\t" +
                            rs.getInt("yr_of_release"));
     }
 } catch (SQLException e) {
     System.out.println(e.getMessage());
 }
}  

}
public class SqlliteConnectivity {

	public static void main(String[] args)throws Exception {
		
        Connection con = null;
		
		try {
			Class.forName("org.sqlite.JDBC");
		    con = DriverManager.getConnection("jdbc:sqlite:test.db");
			System.out.println("SQLite DB connected");
			
		}
		
		catch(Exception e){
			System.out.println(e);
			
		}
		createNewDatabase("hello.db");
		createNewTable();
		
		Movie movie = new Movie();

        movie.insert(101,"Forrest Gump","Tom Hanks","Robin Wright","Robert Zemeckis",1994);
        movie.insert(102,"The Departed","Leonardo Dicaprio","Vera Farmiga","Martin Scorsese",2001);
        movie.insert(103,"Titanic","Leonardo Dicaprio","Kate Winslet","James Cameron",1997);
        System.out.println("items inserted in movies table");
        // To show all the entries in the database
        movie.selectAll();
        System.out.println();
        // Query based on actor name(Parameterized Query)
        movie.ParameterizedQuery("Leonardo Dicaprio");
	}
	public static void createNewDatabase(String fileName) {

        String url = "jdbc:sqlite:D://javaProjects/JDBC/" + fileName;

        try (Connection con = DriverManager.getConnection(url)) {
            if (con != null) {
                DatabaseMetaData meta = con.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
	
	public static void createNewTable() {
		String url = "jdbc:sqlite:D://javaProjects/JDBC/hello.db";
		
		try(Connection con = DriverManager.getConnection(url);
		         Statement stmt = con.createStatement();) 
		        {		      
		          String sql = "CREATE TABLE IF NOT EXISTS movies(movie_id INTEGER primary key,Movie_name VARCHAR(255),Actor VARCHAR(255),Actress VARCHAR(255),Director VARCHAR(255),Yr_Of_Release INTEGER)"; 

		         stmt.executeUpdate(sql);
		         System.out.println("Created table in given database...");   	  
		      } catch (SQLException e) {
		         e.printStackTrace();
		      } 
	
	}
	
}	
