package bigdatasort;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import net.andreinc.mockneat.MockNeat;
import net.andreinc.mockneat.abstraction.MockUnitString;



public class DataGeneration{
 	
 	static Double sizeGb = 1000000.0; //size set as 10gb (10000000000.0)
 		
	private static String getRandomRecord()
 	{
 		MockNeat m = MockNeat.threadLocal();	//mock neat library is used to generate random string
 		String csvline = new String();		
 		
 		 
		Supplier<String> supp = () -> {
		    StringBuilder buff = new StringBuilder();
		    
		    for(int i=0;i<10;i++) {
		  buff .append(m.chars().alphaNumeric().val());
		    }
		  				    
		    return buff.toString();
		};
		
		
		MockUnitString mockUnit = () -> supp;
		
		csvline = m.csvs().column(mockUnit.val()).val();
		
		return csvline;		
			
 	}	
	
	public static void createfile(FileWriter fileWriter, String fileName) throws IOException
		{
		
		System.out.print("Create file method started\n");
		while (true) {
			
			String record = getRandomRecord();
 			
						
			if (new File(fileName).length() > sizeGb) {		//condition to check if file size>10gb
			
				StringBuilder data = new StringBuilder();
				data.append("Generated file \n")
						.append(fileName)
						.append(" at ")
						.append(new SimpleDateFormat("yyyyMMdd_HHmmss")
								.format(Calendar.getInstance().getTime())); 	//create a csv file with strings
				
	 			try {
	 				fileWriter.close();
					break;
								} catch (Exception exception) {
					exception.printStackTrace();
				}
	 			
			}

 			
			try {
				fileWriter.write(record);
				fileWriter.write('\n');
			} catch (Exception exception) {
				exception.printStackTrace();
			}
	}
	}
	
	
	
	public static  void copyFilestoHadoop() throws IOException, URISyntaxException
  	{
  		 System.out.print("\nStarted copying files to Hadoop\n");
  		Configuration conf =new Configuration();
        conf.addResource(new Path("conf/core-site.xml"));
        conf.addResource(new Path("conf/mapred-site.xml"));
        conf.addResource(new Path("conf/hdfs-site.xml"));
        
        FileSystem fs = FileSystem.get( new URI( "hdfs://localhost:9000" ), conf);
                
        Path sourcePath = new Path("D:/eclipse-workspace/Bigdata10GBSort/input/file1.csv");
        
        fs.delete(new Path("hdfs://localhost:9000/Big-Data-Project/hdfs"), true);
        
        if(!(fs.exists(new Path("hdfs://localhost:9000/Big-Data-Project/hdfs"))))
        {
            System.out.println("No Such destination exists :"+"hdfs://localhost:9000/Big-Data-Project/hdfs"+"\n");
            fs.mkdirs(new Path("hdfs://localhost:9000/Big-Data-Project/hdfs"));
            
        }
         
        Path destPath = new Path("hdfs://localhost:9000/Big-Data-Project/hdfs");
        fs.copyFromLocalFile(sourcePath, destPath);
        
        System.out.print("Completed copying file to Hadoop\n");  
  	}
			


}

