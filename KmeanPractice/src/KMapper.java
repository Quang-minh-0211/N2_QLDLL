import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class KMapper extends Mapper<LongWritable, Text, LongWritable, PointWritable>{
	private PointWritable[] currCentroids;
	private final LongWritable centroidId = new LongWritable();
	private final PointWritable pointInput = new PointWritable();
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] arrPropPoint = value.toString().split(",");
		pointInput.set(arrPropPoint);
		double minDistance = Double.MAX_VALUE;
		int centroidIdNearest = 0;
		for(int i=0;i< currCentroids.length; i++) {
			System.out.println("currCentroids[" + i+"]=" + currCentroids[i].toString());
			double distance = pointInput.calcDistance(currCentroids[i]);
			if(distance < minDistance) {
				centroidIdNearest = i;
				minDistance = distance;
			}
		}
		centroidId.set(centroidIdNearest);
		context.write(centroidId, pointInput);
	}
	@Override
	protected void setup(Context context)
	{
		// TODO Auto-generated method stub
		int nClusters = Integer.parseInt(context.getConfiguration().get("k"));
		
		this.currCentroids = new PointWritable[nClusters]; //tao ra mot mang co nCluster
		for(int i=0; i< nClusters;i++) {
			String[] centroid = context.getConfiguration().getStrings("C" + i);
			this.currCentroids[i] = new PointWritable(centroid);
		}
	}
	
	
}
