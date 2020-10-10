import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CoupleWritable implements Writable{

    private Double a;
    private Integer b;

    public CoupleWritable() {
    }

    public CoupleWritable(Double a, Integer b) {
        this.a = a;
        this.b = b;
    }

    public void readFields(DataInput in) throws IOException {
    	a = in.readDouble();
    	b = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(a);
        out.writeInt(b);
    }

    public void set(Double a, Integer b) {
    	 this.a = a;
         this.b = b;
    }
    
    public Double getA(){
        return a;
    }
    
    public Integer getB(){
        return b;
    }
    
    

    @Override
    public String toString() {
        return a.toString() + "\t" + b.toString();
    }

    @Override
    public int hashCode() {
        return a.hashCode() + b.hashCode();
    }


    
}
