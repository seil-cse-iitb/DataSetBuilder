package com.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.csvutils.WriteCSV;
import com.db.ConnectionManager;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SQLQueryExecutor {
	private List<String[]> data;
	private String[] vars= {"TS", "srl", "VA", "W", "VAR", "PF", "A", "F", "V1", "V2", "V3",
			"A1", "A2", "A3", "W1", "W2", "W3", "VA1", "VA2", "VA3", 
			"VAR1", "VAR2", "VAR3", "PF1", "PF2", "PF3", 
			"FwdVAh", "FwdWh", "FwdVARhR", "FwdVARhC"};  
	private String dataDir;
	
	public String getDataDirectory() throws Exception {
		Properties prop = new Properties();
 		InputStream input =  new FileInputStream("./dir.properties");		

 		prop.load(input);
		dataDir=prop.getProperty("data.directory");
		this.dataDir=dataDir.substring(1, dataDir.lastIndexOf("\""));
		System.out.println(this.dataDir);
		return this.dataDir;
	}
	
	public void addHeader(){
		data.add(vars);
	}
	public void writeTable(Connection con, String tableName, String year, int noMonths)
		    throws Exception {

		    Statement stmt = null;
		    String query;
		    ResultSet rs=null;
		    float[] values = new float[vars.length];
		   // String year="2018";
		    List <String> sensIds=getSensorIds(con);
	    	LabelReader labelReader =new LabelReader();
	    	String dirName=getDataDirectory();

		    int resultSetSize=0;
		    float dataPercent;
		   // int noMonths=2;
		    Map<String, MonthCountBean[]> monthCountMap = new HashMap<>();
		    String labelStr, label;
		    for (int j = 0; j < sensIds.size(); j++) {
		    	String temp=sensIds.get(j);
		    	if (!temp.equalsIgnoreCase("power_lcc_202_l") &&
		    		!temp.equalsIgnoreCase("power_lcc_202_p") && !temp.equalsIgnoreCase("power_lcc_23_a") &&
		    		!temp.equalsIgnoreCase("power_lcc_302_l") && !temp.equalsIgnoreCase("power_lcc_302_p")){
		    		
				    	label=labelReader.getLabels(labelReader.readLabels(sensIds),temp);
				    	labelStr=label.substring(1, label.lastIndexOf("\""));
				    	System.out.println(temp+"..."+labelStr);
					    //createDirectory("./data/"+labelStr+"/"+year);
				    	createDirectory(dirName+"/"+labelStr+"/"+year);
					    MonthCountBean[] monthCountArr = new MonthCountBean[noMonths];
					    for (int i=1;i<=noMonths;i++){
					    	data= new ArrayList<String[]>(); 
					    	addHeader();
					    	String query1= "select ";
					    	String query2="";
					    	
					    	for (int k=0;k<vars.length;k++){
					    		query2+=vars[k]+",";	
					    	}
					    	//String fileName="./data/"+labelStr+"/"+year+"/"+getMonthName(i)+".csv";
					    	String fileName=dirName+"/"+labelStr+"/"+year+"/"+getMonthName(i)+".csv";
					    	System.out.println(fileName);
					    	File tmpFile=new File(fileName);
					    	if(tmpFile.exists()) {
					    		System.out.println(fileName+" already exists");
					    		int count=WriteCSV.showNoOfRows(fileName);
					    		dataPercent =  count*100/44640;
					        	String numAsString = String.format ("%.2f", dataPercent);
					        	System.out.println(numAsString+"% data");
					    		monthCountArr[i-1]=new MonthCountBean(getMonthName(i),numAsString);
					    	}else {
						    	String query3= " from " + tableName  
						    			+ " where sensor_id = \""+temp+"\""
								    	+ " and month(from_unixtime(TS))="+i+" and YEAR(FROM_UNIXTIME(TS)) = "+year;//+" limit 200";
						    	
						    	query=query1+query2.substring(0, query2.lastIndexOf(","))+query3;
						    	System.out.println(query);
							     try {
							        stmt = (Statement) con.createStatement();
							        rs = stmt.executeQuery(query);
							        resultSetSize=0;
							        while (rs.next()) {
							        	String[] tempString =new String[vars.length];
							        	for (int l=0;l<vars.length;l++){
							        		values[l]=rs.getFloat(l+1);
							        		//System.out.println(l+"..."+values[l]+"...."+vars[l]);
							        		tempString[l]=String.valueOf(values[l]);				        		
							        	}				        
							        	data.add(tempString);
							        	resultSetSize++;
							        	
							        }
							        String numberAsString="";
								    if(resultSetSize>0){
								    	System.out.println(resultSetSize);
								    	dataPercent =  resultSetSize*100/44640;
							        	numberAsString = String.format ("%.2f", dataPercent);
							        	System.out.println(numberAsString+"% data");
							        }
								    monthCountArr[i-1]=new MonthCountBean(getMonthName(i),numberAsString);
							       
							       // WriteCSV.writeData("./data/"+labelStr+"/"+year+"/"+getMonthName(i)+".csv", data);
							        WriteCSV.writeData(fileName, data);
						     
						        
						     } catch (SQLException e ) {
						    	e.printStackTrace();
						     } finally {
						    	if (stmt != null) { stmt.close(); }
						     }
					    }// end of "if file exists"
					    
				    }//end of for i
		        	monthCountMap.put(temp,monthCountArr);	
		    	}else {//end of if statement
		    		System.out.println("Invalid dataset...");
		    	}
		    }//end of outer for loop (for j)
		    writeStats(monthCountMap,noMonths);
	}
	
	public void writeStats(Map<String, MonthCountBean[]> monthCountMap, int noMonths) {
		List<String[]> stats = new ArrayList<String[]>();
		String[] monthNames= new String[noMonths+1];
		monthNames[0]= "  ";
		for(int i=0;i<noMonths;i++) {
			monthNames[i+1]=getMonthName(i+1);
		}
		stats.add(monthNames);
		
		
	
		for ( String key : monthCountMap.keySet() ) {
	        // System.out.println( key );
						
	        MonthCountBean[] monthCountArr= monthCountMap.get(key);
	        String[] statsValues=new String[monthCountArr.length+1];
	        statsValues[0]=key;
	        int k=1;		        
	        for (MonthCountBean result : monthCountArr) {
		            String count = result.getCount();
		            String month=result.getMonth();
		            System.out.println(key+","+count+","+month);
		            statsValues[k]=count;
		            k++;
	        }
	        stats.add(statsValues);
	     }
		
		WriteCSV.writeData("./data/stats.csv",stats);
		
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
	    	e.printStackTrace();
	    } finally {
	    	if (stmt != null) { stmt.close(); }
	    }
		return sensorIds;
		
	}
	private String getMonthName(int month){
		//String monthName="";
		switch (month)
		{
			case 1: return "January";
			case 2: return "February";
			case 3: return "March";
			case 4: return "April";
			case 5: return "May";
			case 6: return "June";
			case 7: return "July";
			case 8: return "August";
			case 9: return "September";
			case 10: return "October";
			case 11: return "November";
			case 12: return "December";
			default: return "None";
		}	
		
		
	}
	public void createDirectory(String path){
		 File dir = new File(path);
	    
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
    	SQLQueryExecutor sqlExecutor=new SQLQueryExecutor();
    	ConnectionManager connection = new ConnectionManager();
    	//Connection conn=(Connection) connection.getConn("./db.properties");
    	sqlExecutor.writeTable((Connection) connection.getConn("./db.properties"), "agg_sch_3", args[0],Integer.parseInt(args[1]));
    	Instant end = Instant.now();
    	Duration interval = Duration.between(start, end);

		System.out.println("Execution time in seconds: " + interval.getSeconds());
    	connection.closeConnection();
    
    }

}
