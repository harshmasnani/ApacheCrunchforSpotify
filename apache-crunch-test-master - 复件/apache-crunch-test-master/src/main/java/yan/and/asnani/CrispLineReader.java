package yan.and.asnani;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CrispLineReader {
	
	public static Map<String,String> getFileData(String filename){
		Map<String,String> line2map = new HashMap<String,String>();
		
        File file = new File("D:/eclipse-workspace/final_project/apache-crunch-test-master/target/" + filename);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            while ((tempString = reader.readLine()) != null) {
            	String [] splitString = tempString.split(";");
                System.out.println("splitString:  " + splitString[0] + "  price:  " + splitString[1]);
            	line2map.put(splitString[0], splitString[1]);
                line++;
            }
            reader.close();
        }catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
		return line2map;
	} 
	
	public static void writeFileData(String data){
		File file2 =new File("D:/eclipse-workspace/final_project/apache-crunch-test-master/target/EarningData.txt");
		try {
			if(!file2.exists())file2.createNewFile();
		
			FileWriter fileWritter = new FileWriter(file2.getName(),true);
			fileWritter.write(data + "  ");
			fileWritter.close();
			
		}catch(IOException e){
		      e.printStackTrace();
	     }
	      
	}
}
