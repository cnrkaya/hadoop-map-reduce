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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Count {
    
 private static String brand_name;
 private enum COUNTERS{
     
        INVALID_RECORD_COUNT
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");  //csv format
        
        if(args.length != 3){
            System.err.println("Usage:<in> <out> <brandName> ");
            System.exit(2);
        }
        
        brand_name = args[2];
        
        Job job = Job.getInstance(conf,"Calculate viewing numbers of the each product");
        job.setJarByClass(Count.class); 
        job.setMapperClass(CountMapper.class); 
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class); 
        job.setOutputKeyClass(IntWritable.class); 
        job.setOutputValueClass(IntWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
        org.apache.hadoop.mapreduce.Counters counters = job.getCounters(); 
        System.out.println("Invalid record count :"+ counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
        
    }
    
    public static class CountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    IntWritable productId = new IntWritable(); 
    String eventType;
    String brandName;
    IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] parts = value.toString().split("[,]");
            productId.set(Integer.parseInt(parts[TableHeaders.PRODUCT_ID]));
            eventType= parts[TableHeaders.EVENT_TYPE];
            brandName = parts[TableHeaders.BRAND];
                            
            if (parts.length != 9) {
               // add counter for invalid records 
               context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
            }else{
                if( brandName.equals(brand_name) ||brand_name.equals("all") )  
                    if( eventType.equals("view"))
                         context.write(productId, one);
            }

        }
    }
    public static class CountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        
        private IntWritable result = new IntWritable();
        
        @Override
        public void reduce( IntWritable productId,Iterable<IntWritable> values, Context context) 
          throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            result.set(sum);
            context.write(productId, result);

        }
    }
    
    
    
}
