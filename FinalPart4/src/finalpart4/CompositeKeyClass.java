/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author shubhangi
 */
public class CompositeKeyClass implements Writable,
  	WritableComparable<CompositeKeyClass> {

	private String company;
	private String product;

	public CompositeKeyClass() {
	}

	public CompositeKeyClass(String company, String product) {
		this.company = company;
		this.product = product;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(company).append("\t")
				.append(product)).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		company = WritableUtils.readString(dataInput);
		product = WritableUtils.readString(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, company);
		WritableUtils.writeString(dataOutput, product);
	}

	public int compareTo(CompositeKeyClass objKeyPair) {
		// TODO:
		/*
		 * Note: This code will work as it stands; but when CompositeKeyWritable
		 * is used as key in a map-reduce program, it is de-serialized into an
		 * object for comapareTo() method to be invoked;
		 * 
		 * To do: To optimize for speed, implement a raw comparator - will
		 * support comparison of serialized representations
		 */
		int result = company.compareTo(objKeyPair.company);
		if (0 == result) {
			result = product.compareTo(objKeyPair.product);
		}
		return result;
	}

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

	
}