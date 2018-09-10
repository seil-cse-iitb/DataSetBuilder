package com.app;

import java.util.HashMap;
import java.util.Map;

public class DataPercent {
	
	HashMap monthCountMap;
	
	
	 public static void main(String[] args) {
	        Map<String, MonthCountBean[]> monthCountMap = new HashMap<>();
        
	        //MonthCountBean[] arr=new MonthCountBean[4];
	        
	        monthCountMap.put("power_k_m", new MonthCountBean[]{new MonthCountBean("January","20000"), new MonthCountBean("March","25000")});
	        monthCountMap.put("power_k_a", new MonthCountBean[]{new MonthCountBean("February","21000")});
	        //monthCountMap.put("power_k_a", new MonthCountBean("March",25000));
	        //String keyVal;
	        for ( String key : monthCountMap.keySet() ) {
	           // System.out.println( key );
	        
		       // MonthCountBean[] monthCountArr= monthCountMap.get("power_k_m");
	            MonthCountBean[] monthCountArr= monthCountMap.get(key);
		        for (MonthCountBean result : monthCountArr) {
		        	//for(int j=0;j<monthCountArr.length;j++) {
			            String count = result.getCount();
			            String month=result.getMonth();
			            System.out.println(key+","+count+","+month);
		        	//}
		        }
	        }

	        System.out.println(monthCountMap);
	    }

}
