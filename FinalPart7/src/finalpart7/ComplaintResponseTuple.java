/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart7;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author shubhangi
 */
public class ComplaintResponseTuple implements Writable{

    /**
     * @param args the command line arguments
     */
    private long count=0;
    private long timelyCount=0;
    
   
    
    //private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd");

    public long getCount() {
        return count;
    }

    
    

    public long getTimelyCount() {
        return timelyCount;
    }

    public void setTimelyCount(long timelyCount) {
        this.timelyCount = timelyCount;
    }
    

    public void setCount(long count) {
        this.count = count;
    }

    

    
    
    @Override
    public void write(DataOutput d) throws IOException {
       
        d.writeLong(count);
        d.writeLong(timelyCount);
        
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readFields(DataInput di) throws IOException {
                
                count = di.readLong();
                timelyCount=di.readLong();
               
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    

    
    @Override
    public String toString(){
        return count + "\t" + timelyCount ;
        //return stockVolume+ " "+stockAdjClose;
    }
}


