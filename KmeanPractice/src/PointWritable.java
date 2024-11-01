import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PointWritable implements Writable{
	private float[] attributes = null;
	private int dim;
	private int nPoints;
	
	public PointWritable() {
		this.dim=0;
	}
	public PointWritable(final float[] c) {
		this.set(c);
		// dung de khoi tao doi tuong duoi dang mang so thuc
	}
	public PointWritable(final String[] s) {
		this.set(s);
		// dung de khoi tao doi tuong duoi mang cac chuoi
	}
	public static PointWritable copy(final PointWritable p) {
		PointWritable ret = new PointWritable(p.attributes);
		ret.nPoints = p.nPoints;
		return ret;
		// tao mot doi tuong moi tu mot doi tuong duoc truyen vao
	}
	public void set(final float[] c) {
		//thiet lap toa do cho mot diem bang cach nhan vao mot mang so thuc VD(1,2) 
		this.attributes = c;
		this.dim = c.length;
		this.nPoints = 1;
	}
	public void set(final String[] s) {
		//thiet lap toa do cho mot diem bang cach nhan vao mot mang cac chuoi VD(1,2) 
		this.attributes = new float[s.length];
		this.dim = s.length;
		this.nPoints = 1;
		for(int i=0;i<s.length; i++) {
			this.attributes[i] = Float.parseFloat(s[i]);
		}
	}
	@Override
	public void readFields(final DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.dim = in.readInt();
		this.nPoints = in.readInt();
		this.attributes = new float[this.dim];
		
		for(int i=0;i<this.dim; i++) {
			this.attributes[i] = in.readFloat();
		}
	}
	@Override
	public void write(final DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.dim);
		out.writeInt(this.nPoints);
		
		for(int i=0;i < this.dim;i++) {
			out.writeFloat(this.attributes[i]);
		}
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		StringBuilder point = new StringBuilder();
		for(int i=0;i<this.dim; i++) {
			point.append(Float.toString(this.attributes[i]));
			if(i != dim -1) {
				point.append(",");
			}
		}
		return point.toString();
	}
	public void sum(PointWritable p) {
		for(int i=0; i<this.dim; i++) {
			this.attributes[i] += p.attributes[i];
		}
		this.nPoints += p.nPoints;
	}
	public double calcDistance(PointWritable p) {
		double dist = 0.0f;
		for(int i=0; i< this.dim; i++) {
			dist += Math.pow(Math.abs(this.attributes[i] - p.attributes[i]), 2);
		}
		dist = Math.sqrt(dist);
		return dist;
	}
	public void calcAverage() {
		for(int i=0;i<this.dim;i++) {
			float temp = this.attributes[i] / this.nPoints;
			this.attributes[i] = (float)Math.round(temp * 100000) / 100000.0f;
		}
		this.nPoints = 1;
	}
}