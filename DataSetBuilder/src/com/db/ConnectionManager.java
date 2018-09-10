/**
 * 
 */
/**
 * @author Chaitra
 * DbUtils
 */
package com.db;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionManager {

    private String driverName;
   /* private String connectionUrl = "jdbc:mysql://10.129.149.11:3306/seil_sensor_agg_data";
    private String userName = "reader";
    private String userPass = "reader";*/
    private String dbHost,dbName,dbUserName,dbPasswd;
    
    private Connection con = null;

    public ConnectionManager() {

    }
    public Connection getConn(String pathToDbPropertiesFile) throws Exception{
 
    		Properties prop = new Properties();
     		InputStream input =  new FileInputStream(pathToDbPropertiesFile);		
 
     		prop.load(input);
    		this.dbHost=prop.getProperty("mysql.hostname");
    		this.dbName=prop.getProperty("mysql.dbname");
    		this.dbUserName=prop.getProperty("mysql.username");
    		this.dbPasswd=prop.getProperty("mysql.password");
    		this.driverName=prop.getProperty("mysql.drivername");
    		Class.forName(driverName);      		
    		con=DriverManager.getConnection(  
    		"jdbc:mysql://"+dbHost+":3306/"+dbName,dbUserName,dbPasswd);  
    		 System.out.println("Connecting to db..."+dbName+" , "+dbHost +" , "+ dbUserName);
    		return con;

    		}
   /* public Connection createConnection() {
        try {
            con = DriverManager.getConnection(connectionUrl, userName, userPass);
            System.out.println("Connecting to db..."+con.isReadOnly()+" , "+connectionUrl +" , "+ userName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return con;
    }*/

    public void closeConnection() {
        try {
            this.con.close();
            System.out.println("Closing Connection...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
  /*  public static void main(String args[]){
    	ConnectionManager connection = new ConnectionManager();
    	connection.createConnection();
    	connection.closeConnection();
    
    }*/
}

