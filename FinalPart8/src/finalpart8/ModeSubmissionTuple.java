/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author shubhangi
 */
public class ModeSubmissionTuple implements Writable{

    /**
     * @param args the command line arguments
     */
    private long count=0;
    private long emailCount=0;
    private long faxCount=0;
    private long phoneCount=0;
    private long postalCount=0;
    private long mailCount=0;
    private long referralCount=0;
    private long web=0;
    
    
   
    
    //private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd");

    public long getCount() {
        return count;
    }

    public long getEmailCount() {
        return emailCount;
    }

    public void setEmailCount(long emailCount) {
        this.emailCount = emailCount;
    }

    public long getFaxCount() {
        return faxCount;
    }

    public void setFaxCount(long faxCount) {
        this.faxCount = faxCount;
    }

    public long getPhoneCount() {
        return phoneCount;
    }

    public void setPhoneCount(long phoneCount) {
        this.phoneCount = phoneCount;
    }

    public long getPostalCount() {
        return postalCount;
    }

    public void setPostalCount(long postalCount) {
        this.postalCount = postalCount;
    }

    public long getMailCount() {
        return mailCount;
    }

    public void setMailCount(long mailCount) {
        this.mailCount = mailCount;
    }

    public long getReferralCount() {
        return referralCount;
    }

    public void setReferralCount(long referralCount) {
        this.referralCount = referralCount;
    }

    public long getWeb() {
        return web;
    }

    public void setWeb(long web) {
        this.web = web;
    }

   
    

    public void setCount(long count) {
        this.count = count;
    }

    

    
    
    @Override
    public void write(DataOutput d) throws IOException {
       
        d.writeLong(count);
        d.writeLong(emailCount);
        d.writeLong(faxCount);
        d.writeLong(web);
        d.writeLong(mailCount);
        d.writeLong(phoneCount);
        d.writeLong(postalCount);
        d.writeLong(referralCount);
        
        
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readFields(DataInput di) throws IOException {
                
                count = di.readLong();
                emailCount=di.readLong();
                faxCount=di.readLong();
                mailCount=di.readLong();
                referralCount=di.readLong();
                postalCount=di.readLong();
                phoneCount=di.readLong();
                web=di.readLong();
                
               
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    

    
    @Override
    public String toString(){
        return count + "\t" + emailCount + "\t" + mailCount + "\t" + phoneCount + "\t" + postalCount + "\t" + referralCount + "\t" + web + "\t" + faxCount ;
        //return stockVolume+ " "+stockAdjClose;
    }
}



