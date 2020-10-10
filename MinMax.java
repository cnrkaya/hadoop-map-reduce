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


public class MinMax {
    
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
        job.setJarByClass(MinMax.class); 
        job.setMapperClass(MinMaxMapper.class); 
        job.setCombinerClass(MinMaxReducer.class);
        job.setReducerClass(MinMaxReducer.class); 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(CoupleWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
        org.apache.hadoop.mapreduce.Counters counters = job.getCounters(); 
        System.out.println("Invalid record count :"+ counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
        
    }
    
    public static class MinMaxMapper extends Mapper<Object, Text, Text, CoupleWritable> {

    Integer productId ;
    String brandName;
    Double price ;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] parts = value.toString().split("[,]");
            productId = Integer.parseInt(parts[TableHeaders.PRODUCT_ID]);
            price = Double.parseDouble(parts[TableHeaders.PRICE]);
            brandName = parts[TableHeaders.BRAND];
                            
            if (parts.length != 9) {
               // add counter for invalid records 
               context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
            }else{ 
                
                if(brand_option.equals("all") || brandName.equals(brand_option))
                    context.write(new Text(brandName), new CoupleWritable(price,productId));

            }
        }
    }
      
    public static class MinMaxReducer extends Reducer<Text, CoupleWritable, Text, CoupleWritable> {
        DoubleWritable result = new DoubleWritable();
        
        @Override
        public void reduce( Text brandName,Iterable<CoupleWritable> values, Context context) 
          throws IOException, InterruptedException {
            CoupleWritable min = new CoupleWritable(Double.MAX_VALUE,0);
            CoupleWritable max = new CoupleWritable(0.0,0);
            
            for (CoupleWritable val : values) {
                if(val.getA() < min.getA() )
                   min = val;
                if(val.getA() > max.getA())
                    max = val;
            }

            context.write(brandName, min );
            context.write(brandName, max );            

        }
    }
    
    
    
}
