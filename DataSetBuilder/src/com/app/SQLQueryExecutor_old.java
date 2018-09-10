package com.app;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.csvutils.WriteCSV;
import com.db.ConnectionManager;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SQLQueryExecutor_old {
	private List<String[]> data;
	private String[] vars= {"TS", "srl", "VA", "W", "VAR", "PF", "A", "F", "V1", "V2", "V3",
			"A1", "A2", "A3", "W1", "W2", "W3", "VA1", "VA2", "VA3", 
			"VAR1", "VAR2", "VAR3", "PF1", "PF2", "PF3", 
			"FwdVAh", "FwdWh", "FwdVARhR", "FwdVARhC"};  
     
	public void addHeader(){
		/*data.add(new String[] { "TS", "srl", "VA", "W", "VAR", "PF", "A", "F", "V1", "V2", "V3",
				"A1", "A2", "A3", "W1", "W2", "W3", "VA1", "VA2", "VA3", 
				"VAR1", "VAR2", "VAR3", "PF1", "PF2", "PF3", 
				"FwdVAh", "FwdWh", "FwdVARhR", "FwdVARhC" });*/
		data.add(vars);
		//this.data=data;
	}
	public void writeTable(Connection con, String tableName)
		    throws SQLException {

		    Statement stmt = null;
		    String query;
		    ResultSet rs=null;
		    //float ts, w;
		  /*  float TS, srl, VA, W, VAR, PF, A, F, V1, V2, V3, A1, A2, A3, W1, W2, W3;
		    float VA1, VA2, VA3, VAR1, VAR2, VAR3, PF1, PF2, PF3, FwdVAh, FwdWh, FwdVARhR, FwdVARhC;*/
		    float[] values = new float[30];
		    String year="2018";
		    List <String> sensIds=getSensorIds(con);
		    for (int j = 0; j < 3; j++) {
		    	String temp=sensIds.get(j);
		    	System.out.println(temp);
		    //for (String temp : sensIds) {
				//System.out.println(temp);
			    createDirectory("./data/"+temp+"/"+year);
			    for (int i=1;i<=2;i++){
			    	data= new ArrayList<String[]>(); 
			    	addHeader();
	   		        //query= "select TS, W from " + tableName + " where sensor_id = \""+temp+"\""
				    	//	+ " and month(from_unixtime(TS))="+i+" and YEAR(FROM_UNIXTIME(TS)) = "+year;
			    	
			    	/*query= "select TS, srl, VA, W, VAR, PF, A, F, V1, V2,"
			    			+ " V3, A1, A2, A3, W1, W2, W3, VA1, VA2, VA3, VAR1, "
			    			+ "VAR2, VAR3, PF1, PF2, PF3, FwdVAh, FwdWh, FwdVARhR, FwdVARhC from " + tableName  
			    			+ " where sensor_id = \""+temp+"\""
					    	+ " and month(from_unixtime(TS))="+i+" and YEAR(FROM_UNIXTIME(TS)) = "+year;*/
			    	
			    	String query1= "select ";
			    	String query2="";
			    	for (int k=0;k<vars.length;k++){
			    		query2+=vars[k]+",";	
			    	}
			    	
			    	String query3= " from " + tableName  
			    			+ " where sensor_id = \""+temp+"\""
					    	+ " and month(from_unixtime(TS))="+i+" and YEAR(FROM_UNIXTIME(TS)) = "+year;
			    	
			    	query=query1+query2.substring(0, query2.lastIndexOf(","))+query3;
			    	System.out.println(query);
				     try {
				        stmt = (Statement) con.createStatement();
				        rs = stmt.executeQuery(query);
				        while (rs.next()) {
				       /*     TS = rs.getFloat("TS");
				            srl = rs.getFloat("srl");
				            VA= rs.getFloat("VA");
				            W= rs.getFloat("W");
				            VAR= rs.getFloat("VAR");
				            PF= rs.getFloat("PF");
				            A= rs.getFloat("A");
				            F= rs.getFloat("F");
				            V1= rs.getFloat("V1");
				            V2= rs.getFloat("V2");
				            V3= rs.getFloat("V3");
				            A1= rs.getFloat("A1");
				            A2= rs.getFloat("A2");
				            A3= rs.getFloat("A3");
				            W1= rs.getFloat("W1");
				            W2= rs.getFloat("W2");
				            W3= rs.getFloat("W3");
				            VA1= rs.getFloat("VA1");
				            VA2= rs.getFloat("VA2");
				            VA3= rs.getFloat("VA3");
				            VAR1= rs.getFloat("VAR1");
				            VAR2= rs.getFloat("VAR2");
				            VAR3= rs.getFloat("VA3");
				            PF1= rs.getFloat("PF1");
				            PF2= rs.getFloat("PF2");
				            PF3= rs.getFloat("PF3");
				            FwdVAh= rs.getFloat("FwdVAh");
				            FwdWh= rs.getFloat("FwdWh");
				            FwdVARhR= rs.getFloat("FwdVARhR");
				            FwdVARhC= rs.getFloat("FwdVARhC"); 
				            //System.out.println(ts+ "\t" + w);*/
				        	String[] tempString =new String[30];
				        	for (int l=0;l<vars.length;l++){
				        		values[l]=rs.getFloat(l+1);
				        		System.out.println(l+"..."+values[l]+"...."+vars[l]);
				        		tempString[l]=String.valueOf(values[l]);				        		
				        	}				        
				        	data.add(tempString);
				        	
				           /* data.add(new String[] {
				            		String.valueOf(TS), String.valueOf(srl), String.valueOf(VA), String.valueOf(W), String.valueOf(VAR), 
				            		String.valueOf(PF), String.valueOf(A), String.valueOf(F), String.valueOf(V1), String.valueOf(V2), 
				            		String.valueOf(V3), String.valueOf(A1), String.valueOf(A2), String.valueOf(A3), 
				            		String.valueOf(W1), String.valueOf(W2), String.valueOf(W3), String.valueOf(VA1), String.valueOf(VA2), String.valueOf(VA3),
				            		String.valueOf(VAR1), String.valueOf(VAR2), String.valueOf(VAR3), 
				            		String.valueOf(PF1), String.valueOf(PF2), String.valueOf(PF3),
				            		String.valueOf(FwdVAh), String.valueOf(FwdWh), String.valueOf(FwdVARhR), String.valueOf(FwdVARhC) 
				            		});*/
				        	
				        }
				        WriteCSV.writeData("./data/"+temp+"/"+year+"/"+i+".csv", data);
				     } catch (SQLException e ) {
			      //  JDBCTutorialUtilities.printSQLException(e);
				    	e.printStackTrace();
				     } finally {
				    	if (stmt != null) { stmt.close(); }
				     }
			    }//end of for i
		    }//end of outer for loop
	}
	public List <String> getSensorIds(Connection con) throws SQLException{
		List <String> sensorIds=new ArrayList<String>();
		ResultSet res=null;
		Statement stmt = null;
		String sen_id;
		String query="select distinct sensor_id from agg_sch_3";
		try {
	        stmt = (Statement) con.createStatement();
	        res = stmt.executeQuery(query);
	        while (res.next()) {
	            sen_id = res.getString("sensor_id");
	            System.out.println("sensor_id..."+sen_id);
	            sensorIds.add(sen_id);
	        }
		} catch (SQLException e ) {
		      //  JDBCTutorialUtilities.printSQLException(e);
	    	e.printStackTrace();
	    } finally {
	    	if (stmt != null) { stmt.close(); }
	    }
		return sensorIds;
		
	}
	
	public void createDirectory(String path){
		 File dir = new File(path);
	 //   File dir = new File("/Users/al/tmp/TestDirectory");
	    
	    // attempt to create the directory here
	    boolean successful = dir.mkdirs();
	    if (successful)
	    {
	      // creating the directory succeeded
	      System.out.println("directory was created successfully");
	    }
	    else
	    {
	      // creating the directory failed
	      System.out.println("failed trying to create the directory");
	    }
	  }
	
    public static void main(String args[]) throws Exception{
    	Instant start = Instant.now();
    	SQLQueryExecutor_old sqlExecutor=new SQLQueryExecutor_old();
    	ConnectionManager connection = new ConnectionManager();
    	//Connection conn=(Connection) connection.createConnection();
    	//sqlExecutor.getSensorIds(conn);
    	sqlExecutor.writeTable((Connection) connection.getConn("./db.properties"), "agg_sch_3");
    	Instant end = Instant.now();
    	Duration interval = Duration.between(start, end);

		System.out.println("Execution time in seconds: " + interval.getSeconds());
		//System.out.println("Execution time in seconds: " + interval.get(SECONDS));
    	connection.closeConnection();
    
    }

}
