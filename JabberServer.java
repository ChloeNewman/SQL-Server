import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;

public class JabberServer {

    private static String dbcommand = "jdbc:postgresql://127.0.0.1:5432/postgres";
    private static String db = "postgres";
    private static String pw = "";

    private static Connection conn;

    public static Connection getConnection() {
        return conn;
    }

    public static void main(String[] args) {

        JabberServer jabber = new JabberServer();
        JabberServer.connectToDatabase();
        jabber.resetDatabase();
    }

    public ArrayList<String> getFollowerUserIDs(int userid) {
        //initialise the arraylist that will return the data
        ArrayList<String> followerUserIDs = new ArrayList<String>();

        //create the string which holds the SQL query
        String query = "SELECT userida FROM follows WHERE useridb = " + userid;

        //try and catch statement
        try {
            //create the statement and dataset
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet data = stmt.executeQuery();

            //process the data
            while (data.next()) {
                followerUserIDs.add(data.getString("userida"));
            }

            //System.out.println(followerUserIDs);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Return the data arraylist
        return followerUserIDs;
    }

    public ArrayList<String> getFollowingUserIDs(int userid) {
        //initialise the arraylist that will return the data
        ArrayList<String> followingUserIDs = new ArrayList<String>();

        //create the string which holds the SQL query
        String query = "SELECT useridb FROM follows WHERE userida = " + userid;

        //try and catch statement
        try {
            //create the statement and dataset
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet data = stmt.executeQuery();

            //process the data
            while (data.next()) {
                followingUserIDs.add(data.getString("useridb"));
            }

            //System.out.print(followingUserIDs);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Return the data arraylist
        return followingUserIDs;
    }

    public ArrayList<ArrayList<String>> getMutualFollowUserIDs() {
        //initialise the arraylist that will return the data
        ArrayList<ArrayList<String>> mutualFollowIDs = new ArrayList<ArrayList<String>>();

        //create the string which holds the SQL query
        String query = "SELECT userida, useridb FROM follows INNER JOIN (SELECT userida AS a, useridb AS b FROM follows) AS mirror ON follows.userida = mirror.b AND follows.useridb = mirror.a WHERE userida > useridb";

        //try and catch statement
        try {
            //create the statement and dataset
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet data = stmt.executeQuery();

            //process the data
            while (data.next()) {
                //data arraylist that will be added to
                ArrayList<String> dataloop = new ArrayList<String>();
                dataloop.add(data.getString("userida"));
                dataloop.add(data.getString("useridb"));

                mutualFollowIDs.add(dataloop);
            }
        //System.out.print(mutualFollowIDs);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Return the data arraylist
        return mutualFollowIDs;
    }

    public ArrayList<ArrayList<String>> getLikesOfUser(int userid) {
        //initialise the arraylist that will return the data
        ArrayList<ArrayList<String>> userLikes = new ArrayList<ArrayList<String>>();

        //create the string which holds the SQL query
        String query = "SELECT jabberuser.username, jab.jabtext FROM jab NATURAL JOIN jabberuser NATURAL JOIN (SELECT jabid FROM likes WHERE userid = " + userid + ") AS j";

        //try and catch statement
        try {
            //create the statement and dataset
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet data = stmt.executeQuery();

            //process the data
            while (data.next()) {
                //data arraylist that will be added to
                ArrayList<String> dataloop = new ArrayList<String>();
                dataloop.add(data.getString("username"));
                dataloop.add(data.getString("jabtext"));

                userLikes.add(dataloop);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Return the data arraylist
        return userLikes;
    }

    public ArrayList<ArrayList<String>> getTimelineOfUser(int userid) {
        //initialise the arraylist that will return the data
        ArrayList<ArrayList<String>> userTimeline = new ArrayList<ArrayList<String>>();

        //create the string which holds the SQL query
        String query = "SELECT jabberuser.username, jab.jabtext FROM jab " +
                "NATURAL JOIN jabberuser NATURAL JOIN " +
                "(SELECT useridb AS userid " +
                "FROM follows " +
                "WHERE userida = " + userid + ") AS u";

        //try and catch statement
        try {
            //create the statement and dataset
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet data = stmt.executeQuery();

            //process the data
            while (data.next()) {
                //data arraylist that will be added to
                ArrayList<String> dataloop = new ArrayList<String>();
                dataloop.add(data.getString("username"));
                dataloop.add(data.getString("jabtext"));

                userTimeline.add(dataloop);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Return the data arraylist
        return userTimeline;
    }

    public void addJab(String username, String jabtext) {
        //create the string which holds the SQL query
        String quory = "SELECT userid FROM jabberuser WHERE username = '" + username + "'";
        String query = "SELECT MAX(jabid) FROM jab";

        //try and catch statement
        try {
            //create the statement and dataset
            PreparedStatement stmt = conn.prepareStatement(quory);
            ResultSet data = stmt.executeQuery();

            PreparedStatement stmtTwo = conn.prepareStatement(query);
            ResultSet dataTwo = stmtTwo.executeQuery();

            //initialise maxjabid and userid
            int maxjabid = 0;
            int useridd = 0;

            //process the data
            while (data.next()) {
                //get the userid
                useridd = data.getInt("userid");

                while (dataTwo.next()) {

                    //get the highest id number and add 1
                    maxjabid = (dataTwo.getInt("max")) + 1;

                    //add new user
                    String newUser = "INSERT INTO jab (VALUES (" + maxjabid + ", '" + useridd + "', '" + jabtext + "'))";

                    PreparedStatement stmtThree = conn.prepareStatement(newUser);
                    stmtThree.executeUpdate();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void addUser(String username, String emailadd) {
        //create the string which holds the SQL query
        String query = "SELECT MAX(userid) FROM jabberuser";

        //try and catch statement
        try {
            //create the statement and dataset
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet data = stmt.executeQuery();

            //initialise maxid
            int maxid = 0;

            //process the data
            while (data.next()) {
                //get the highest id number and add 1
                maxid = (data.getInt("max")) + 1;

                //add new user
                String newUser = "INSERT INTO jabberuser (VALUES (" + maxid + ", '" + username + "', '" + emailadd + "'))";
                PreparedStatement stmtTwo = conn.prepareStatement(newUser);
                stmtTwo.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void addFollower(int userida, int useridb) {
        //create the string which holds the SQL query
        String query = "INSERT INTO follows (VALUES (" + userida + ", " + useridb + "))";

        //try and catch statement
        try {
            //create the statement and dataset
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void addLike(int userid, int jabid) {
        //create the string which holds the SQL query
        String query = "INSERT INTO likes (VALUES (" + userid + ", " + jabid + "))";

        //try and catch statement
        try {
            //create the statement and dataset
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<String> getUsersWithMostFollowers() {
        //initialise the arraylist that will return the data
        ArrayList<String> mostFollowers = new ArrayList<String>();

        //create the string which holds the SQL query
        String query = "SELECT useridb FROM follows " +
                "GROUP BY useridb HAVING COUNT(userida) >= ALL (" +
                "SELECT COUNT(userida) FROM follows " +
                "GROUP BY useridb " +
                "ORDER BY COUNT(userida) DESC)";

        //try and catch statement
        try {
            //create the statement and dataset
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet data = stmt.executeQuery();

            //process the data
            while (data.next()) {
                mostFollowers.add(data.getString("useridb"));
            }

            //System.out.print(mostFollowers);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Return the data arraylist
        return mostFollowers;
    }

    /*
     * Code provided for the assignment is redeacted. This comprises code that connects that connects to the postgres database.
     * The methods provided:
     *      connectToDatabase()
     *      resetDatabase()
     *      dropTables()
     *      loadSQL()
     *      executeSQLUpdates()
     *      JabberServer()
     *      print1() and print2() [print ArrayList and ArrayList of ArrayList<String>s to console
     */
}
