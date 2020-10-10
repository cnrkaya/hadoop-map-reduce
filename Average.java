/**
 *finding the cheapest and most expensive products of the given brand
 */

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Average {
    
 private static String brand_option;
 private enum COUNTERS{
     
        INVALID_RECORD_COUNT
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");  //csv format
        
        if(args.length != 3){
            System.err.println("Usage:<in> <out> <brandOpt> ");
            System.exit(2);
        }
        
        brand_option = args[2];
        
        Job job = Job.getInstance(conf,"Min-Max");
        job.setJarByClass(Average.class); 
        job.setMapperClass(AverageMapper.class); 
        job.setCombinerClass(AverageReducer.class);
        job.setReducerClass(AverageReducer.class); 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(DoubleWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
        org.apache.hadoop.mapreduce.Counters counters = job.getCounters(); 
        System.out.println("Invalid record count :"+ counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
        
    }
    
    public static class AverageMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    IntWritable productId = new IntWritable(); 
    String brandName;
    DoubleWritable price = new DoubleWritable();
    String eventType;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] parts = value.toString().split("[,]");
            productId.set(Integer.parseInt(parts[TableHeaders.PRODUCT_ID]));
            price.set(Double.parseDouble(parts[TableHeaders.PRICE]));
            brandName = parts[TableHeaders.BRAND];
            eventType = parts[TableHeaders.EVENT_TYPE];     
            
            if (parts.length != 9) {
               // add counter for invalid records 
               context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
            }else{ 
                if(eventType.equals("purchase")){
                    if(brand_option.equals("all"))
                        context.write(new Text(brandName), price);
                    else if( brandName.equals(brand_option))
                         context.write(new Text(brandName), price);
                }
            }
        }
    }
      
    public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        
        @Override
        public void reduce( Text brandName,Iterable<DoubleWritable> values, Context context) 
          throws IOException, InterruptedException {
            Double sum = 0.0;
            Integer size = 0;
            Double average;
            
            for (DoubleWritable val : values) {
                sum += val.get();
                size += 1;
            }
            average = sum/size;

            context.write(brandName, new DoubleWritable(average) );
         

        }
    }
    
    
    
}
