/**
 *finding the cheapest and most expensive products of the given brand
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class StdDeviation {
    
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
        job.setJarByClass(StdDeviation.class); 
        job.setMapperClass(StdDeviationMapper.class); 
        job.setCombinerClass(StdDeviationReducer.class);
        job.setReducerClass(StdDeviationReducer.class); 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(DoubleWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
        org.apache.hadoop.mapreduce.Counters counters = job.getCounters(); 
        System.out.println("Invalid record count :"+ counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
        
    }
    
    public static class StdDeviationMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    IntWritable productId = new IntWritable(); 
    String brandName;
    DoubleWritable price = new DoubleWritable();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] parts = value.toString().split("[,]");
            productId.set(Integer.parseInt(parts[TableHeaders.PRODUCT_ID]));
            price.set(Double.parseDouble(parts[TableHeaders.PRICE]));
            brandName = parts[TableHeaders.BRAND];

            
            if (parts.length != 9) {
               // add counter for invalid records 
               context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
            }else{ 

                if(brand_option.equals("all"))
                    context.write(new Text(brandName), price);
                else if( brandName.equals(brand_option))
                     context.write(new Text(brandName), price);
                
            }
        }
    }
      
    public static class StdDeviationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        
        public List<Double> list = new ArrayList<Double>();

        
        @Override
        public void reduce( Text brandName,Iterable<DoubleWritable> values, Context context) 
          throws IOException, InterruptedException {
            
            double sum = 0;
            double count = 0;
            list.clear();

            for (DoubleWritable doubleWritable : values) {
                sum = sum + doubleWritable.get();
                count = count + 1;
                list.add(doubleWritable.get());
            }
            
            double sd = 0.0;
            double mean = sum / count;
            double sumOfSquares = 0;
            Collections.sort(list);
            for (double doubleWritable : list) {
                sumOfSquares += (doubleWritable - mean) * (doubleWritable - mean);
            }
            sd = (double) Math.sqrt(sumOfSquares / (count - 1));
            context.write(brandName, new DoubleWritable(sd));

        }
     }

        

}
        
