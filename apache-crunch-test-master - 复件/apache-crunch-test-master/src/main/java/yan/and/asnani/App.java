package yan.and.asnani;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Target.WriteMode;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class App extends Configured implements Tool {
	
	private String SINGER = "";

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new App(), args);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Execute: hadoop jar crunch-job.jar input output");
            System.err.println();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return 1;
        }

        String inputPath = args[0];
        String input2Path = args[1];
        String outputPath = args[2];

        Pipeline pipeline = new MRPipeline(App.class, getConf());

        PCollection<String> lines = pipeline.readTextFile(inputPath);    

        PCollection<String> singerSongList = lines.parallelDo(new SplitColDoFN(1), Writables.strings());
        
     
   
        PTable<String, Long> singerSongAmount;
        PTable<String, Long> singerSongAmountClone;

        if(SINGER.equals("") || SINGER == null) {
        	singerSongAmount = singerSongList.count();
        }else {
        	singerSongAmount = singerSongList.filter(new KeyWordFilter(SINGER)).count();
        }
        
        singerSongAmount.cache();

        pipeline.write(singerSongAmount, At.textFile(outputPath), WriteMode.APPEND);

        
        PipelineExecution pe = pipeline.runAsync();


        PipelineResult result = pe.get();
        

        System.out.println("Pipeline.dot: " + pe.getPlanDotFile());
        

        String dot = pipeline.getConfiguration().get("crunch.planner.dotfile");
        Files.write(dot, new File("pipeline.dot"), Charsets.UTF_8);
        
        
        Map userMap = singerSongAmount.materializeToMap();
        
        Map priceMap = CrispLineReader.getFileData(input2Path);
        Iterator nameIterator = priceMap.keySet().iterator();

    	System.out.println("name   price");
        while(nameIterator.hasNext()) {
        	String name = (String)nameIterator.next();
         	Long count = (Long)userMap.get(name);
         	Double price = Double.parseDouble((String)priceMap.get(name));
        	Double amount = count * price;
        	
        	String resultData =  name +": "+ amount;
        	System.out.println(resultData);
        	
        	CrispLineReader.writeFileData(resultData);
        	
        }
      
        
        return result.succeeded() ? 0 : 1;
    }
}
