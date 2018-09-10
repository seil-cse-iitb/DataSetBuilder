/**
 * 
 */
/**
 * @author Chaitra
 *
 */
package com.csvutils;

//Java program to illustrate
//for Writing Data in CSV file
import java.io.*;
import java.util.*;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

public class WriteCSV {
 private static final String CSV_FILE_PATH= "./result.csv";
 public static void main(String[] args)
 {
	 List<String[]> data = new ArrayList<String[]>();
     data.add(new String[] { "Name", "Class", "Marks" });
     data.add(new String[] { "Aman", "10", "620" });
     data.add(new String[] { "Suraj", "10", "630" });
	 writeData(CSV_FILE_PATH, data);
	 showNoOfRows(CSV_FILE_PATH);
 }
 public static void writeData(String filePath, List <String[]> data)
 {
  
     // first create file object for file placed at location
     // specified by filepath
     File file = new File(filePath);
  
     try {
         // create FileWriter object with file as parameter
         FileWriter outputfile = new FileWriter(file);
         // create CSVWriter object filewriter object as parameter
         CSVWriter writer = new CSVWriter(outputfile);
         writer.writeAll(data);
         // closing writer connection
         writer.close();
     }
     catch (IOException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
     }
 }
 public static int showNoOfRows(String filePath) {
	 int size = 0;
	     try {
	         // Create an object of file reader
	         // class with CSV file as a parameter.
	         FileReader filereader = new FileReader(filePath);
	  
	         // create csvReader object and skip first Line
	         CSVReader csvReader = new CSVReaderBuilder(filereader)
	                                   .withSkipLines(1)
	                                   .build();
	         List<String[]> allData = csvReader.readAll();
	  
	         // print Data
	         /*for (String[] row : allData) {
	             for (String cell : row) {
	                 System.out.print(cell + "\t");
	             }
	             System.out.println();
	         }*/
	         size=allData.size();
	         System.out.println("No. of Rows ..."+size);
	         
	     }
	     catch (Exception e) {
	         e.printStackTrace();
	     }
	     return size;
	 }
 
 public static void addDataToCSV(String output)
 {
     // first create file object for file placed at location
     // specified by filepath
     File file = new File(output);
     Scanner sc = new Scanner(System.in);
     try {
         // create FileWriter object with file as parameter
         FileWriter outputfile = new FileWriter(file);

         // create CSVWriter with ';' as separator
         CSVWriter writer = new CSVWriter(outputfile, ';',
                                          CSVWriter.NO_QUOTE_CHARACTER,
                                          CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                                          CSVWriter.DEFAULT_LINE_END);

         // create a List which contains Data
         List<String[]> data = new ArrayList<String[]>();

         System.out.println("Enter no of rows");
         int noOfRow = Integer.parseInt(sc.nextLine());
         System.out.println("Enter Data");
         for (int i = 0; i < noOfRow; i++) {
             String row = sc.nextLine();
             String[] rowdata = row.split(" ");
             data.add(rowdata);
         }

         writer.writeAll(data);

         // closing writer connection
         writer.close();
     }
     catch (IOException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
     }
 }
}