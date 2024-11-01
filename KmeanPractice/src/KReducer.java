import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KReducer extends Reducer<LongWritable, PointWritable, Text, Text>{
	private final Text newCentroidId = new Text();
	private final Text newCentroidValue = new Text();
	
	public void reduce(LongWritable centroidId, Iterable<PointWritable> partialSums, Context context)
	throws IOException, InterruptedException{
		PointWritable ptFinalSum = PointWritable.copy(partialSums.iterator().next());
		while(partialSums.iterator().hasNext()) {
			ptFinalSum.sum(partialSums.iterator().next());
		}
		ptFinalSum.calcAverage();
		
		newCentroidId.set(centroidId.toString());
		newCentroidValue.set(ptFinalSum.toString());
		context.write(newCentroidId, newCentroidValue);
	}
}
