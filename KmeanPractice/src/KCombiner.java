import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KCombiner extends Reducer<LongWritable, PointWritable, LongWritable, PointWritable>{
	public void reduce(LongWritable centroidId, Iterable<PointWritable> points, Context context)
	throws IOException, InterruptedException{
		PointWritable ptSum = PointWritable.copy(points.iterator().next()); // lay phan tu dau tien trong mang point de lam tong cua cac toa do diem
		while(points.iterator().hasNext()) {
			ptSum.sum(points.iterator().next());
		}
		context.write(centroidId, ptSum);
	}
}
