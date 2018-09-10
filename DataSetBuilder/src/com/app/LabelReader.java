package com.app;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class LabelReader {

	private Map<String, String> labelMap=new HashMap<>();
	public Map<String, String> readLabels(List <String> sensorIds) throws Exception {
		Properties prop = new Properties();
 		InputStream input =  new FileInputStream("./datalabels.properties");		

 		prop.load(input);
 		for ( int i=0;i<sensorIds.size();i++ ) {
 			String key=sensorIds.get(i);
 			labelMap.put(key, prop.getProperty(key));
 		}
		return labelMap;
	}
	
	public String getLabels(Map <String, String> labelMap, String value) {
		/*for(String key : labelMap.keySet()) {
		}*/
		System.out.println(labelMap.get(value));
		return labelMap.get(value);
	}
}
