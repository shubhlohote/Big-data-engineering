/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author shubhangi
 */
public class MinTuple implements Writable{

    /**
     * @param args the command line arguments
     */
    
    private Date year = new Date();
    private long count=0;
   
    
    //private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd");

    public Date getYear() {
        return year;
    }

    public void setYear(Date year) {
        this.year = year;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }


    
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeLong(year.getTime());
  
        d.writeLong(count);
     
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readFields(DataInput di) throws IOException {
                year = new Date(di.readLong());
                count = di.readLong();
                
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    

    
    @Override
    public String toString(){
        return year + "\t" + count;
        //return stockVolume+ " "+stockAdjClose;
    }
}


