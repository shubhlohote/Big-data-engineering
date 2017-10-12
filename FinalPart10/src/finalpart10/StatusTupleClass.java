/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart10;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author shubhangi
 */
public class StatusTupleClass implements Writable{

    /**
     * @param args the command line arguments
     */
    private long count=0;
    private long closedCount=0;
    private long monCount=0;
    private long nonMonCount=0;
    private long untimelyCount=0;
    
   
    
    //private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd");

    public long getCount() {
        return count;
    }

    public long getClosedCount() {
        return closedCount;
    }

    public void setClosedCount(long closedCount) {
        this.closedCount = closedCount;
    }

    public long getMonCount() {
        return monCount;
    }

    public void setMonCount(long monCount) {
        this.monCount = monCount;
    }

    public long getNonMonCount() {
        return nonMonCount;
    }

    public void setNonMonCount(long nonMonCount) {
        this.nonMonCount = nonMonCount;
    }

    public long getUntimelyCount() {
        return untimelyCount;
    }

    public void setUntimelyCount(long untimelyCount) {
        this.untimelyCount = untimelyCount;
    }
    

    
    

   
    public void setCount(long count) {
        this.count = count;
    }

    

    
    
    @Override
    public void write(DataOutput d) throws IOException {
       
        d.writeLong(count);
        d.writeLong(closedCount);
        d.writeLong(monCount);
        d.writeLong(nonMonCount);
        
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readFields(DataInput di) throws IOException {
                
                count = di.readLong();
                closedCount=di.readLong();
                monCount=di.readLong();
                nonMonCount=di.readLong();
               
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    

    
    @Override
    public String toString(){
        return count + "\t" + closedCount +"\t" + monCount +"\t" + nonMonCount
                +"\t" + untimelyCount;
        //return stockVolume+ " "+stockAdjClose;
    }
}


