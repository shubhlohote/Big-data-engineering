/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author shubhangi
 */
public class CompanyStateTuple implements Writable{

    /**
     * @param args the command line arguments
     */
    private long count=0;
    private String company;
    
    //private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd");

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    
    
    @Override
    public void write(DataOutput d) throws IOException {
       
        d.writeLong(count);
        d.writeBytes(company);
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readFields(DataInput di) throws IOException {
                
                count = di.readLong();
                company = di.readLine();
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    

    
    @Override
    public String toString(){
        return count + "\t" + company;
        //return stockVolume+ " "+stockAdjClose;
    }
}


