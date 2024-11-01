import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
	public static PointWritable[] initRandomCentroids(int kClusters, int nLineOfInputFile, String inputFilePath, Configuration conf)
	throws IOException{
		System.out.println("Initializing random " + kClusters + " centroids...");
		PointWritable[] points = new PointWritable[kClusters]; //mang chua cac tam duoc khoi tao
		
		List<Integer> lstLinePos = new ArrayList<Integer>();// danh sach vi tri dong se dc khoi tao
		Random random = new Random();
		int pos;
		while(lstLinePos.size() < kClusters) {
			pos = random.nextInt(nLineOfInputFile); //tam se dc lay ngau nhien trong n dong du lieu cua fileInput
			if(!lstLinePos.contains(pos)) {
				lstLinePos.add(pos); // dam bao moi dong dc chon la duy nhat khong co 2 tam giong nhau
			}
		}
		Collections.sort(lstLinePos);
		
		FileSystem hdfs = FileSystem.get(conf);
		FSDataInputStream in = hdfs.open(new Path(inputFilePath));
		BufferedReader br = new BufferedReader(new InputStreamReader(in)); //doc noi dung cua file theo dong
		
		int row = 0; // theo doi dong dang doc o hien tai
		int i=0;
		while(i<lstLinePos.size()) {
			pos = lstLinePos.get(i);
			String point = br.readLine();
			if(row==pos) {
				points[i] = new PointWritable(point.split(","));
				i++;
			}
			row++;
		}
		br.close();
		return points;
	}
	public static void saveCentroidsForShared(Configuration conf, PointWritable[] points) {
		for(int i=0;i< points.length;i++) {
			String centroidName = "C"+i;
			conf.unset(centroidName); // xoa gia tri cu cua tam cum neu no ton tai
			conf.set(centroidName, points[i].toString()); //thiet lap gia tri moi cho tam cum
		}
	}
	public static PointWritable[] readCentroidsFromReducerOutput(Configuration conf, int kClusters, String folderOutputPath)
	throws IOException, FileNotFoundException{
		PointWritable[] points = new PointWritable[kClusters];
		FileSystem hdfs = FileSystem.get(conf);
		FileStatus[] status = hdfs.listStatus(new Path(folderOutputPath)); // lay ket qua cua reducer
		
		for(int i=0;i<status.length;i++) {
			if(!status[i].getPath().toString().endsWith("_SUCCESS")) {
				Path outFilePath = status[i].getPath();
				System.out.println("read " + outFilePath.toString()); //mo va doc du lieu trong file bo qua _SUCCESS
				BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(outFilePath)));
				String line = null;
				while((line = br.readLine()) != null) {
					System.out.println(line);
					
					String[] strCentroidInfo = line.split("\t");
					int centroidId = Integer.parseInt(strCentroidInfo[0]); // luu id tam cum
					String[] attrPoint = strCentroidInfo[1].split(","); // luu toa do tam cum
					points[centroidId] = new PointWritable(attrPoint);
					
				}
				br.close();
			}
		}
		hdfs.delete(new Path(folderOutputPath), true); //xoa file ket qua luon
		return points;
	}
		private static boolean checkStopKmean(PointWritable[] oldCentroids, PointWritable[] newCentroids, float threshold) {
			boolean needStop = true;
			System.out.println("Check for stop K-means if distance <= "+ threshold);
			for(int i=0;i<oldCentroids.length;i++) {
				double dist = oldCentroids[i].calcDistance(newCentroids[i]);
				System.out.println("distance centroid["+i+"] changed: "+dist+" (threshold:"+threshold+")");
				needStop = dist <= threshold;
				if(!needStop) {
					return false;
				}
			}
			return true;
		}
		private static void writeFinalResult(Configuration conf, PointWritable[] centroidsFound, String outputFilePath, PointWritable[] centroidsInit)
		throws IOException{
			FileSystem hdfs = FileSystem.get(conf);
			FSDataOutputStream dos = hdfs.create(new Path(outputFilePath), true);
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));
			
			for(int i=0;i<centroidsFound.length;i++) {
				br.write(centroidsFound[i].toString());
				br.newLine();
				System.out.println("Centroid["+i+"]: ("+centroidsFound[i] + ") init: ("+ centroidsInit[i]+")");
			}
			br.close();
			hdfs.close();
		}
		public static PointWritable[] copyCentroids(PointWritable[] points) {
			PointWritable[] savePoints = new PointWritable[points.length];
			for(int i=0;i<savePoints.length;i++) {
				savePoints[i] = PointWritable.copy(points[i]);
			}
			return savePoints;
		}
		public static int MAX_LOOP = 50;
		
		public static void printCentroids(PointWritable[] points, String name) {
			System.out.println("=> CURRENT CENTROIDS: ");
			for(int i=0;i<points.length;i++) {
				System.out.println("centroids("+name+")[" + i + "]=> :" +points[i]);
			}
		}
		public static void assignPointsToCentroids(Configuration conf, String inputFilePath, PointWritable[] centroids, String outputFilePath)
		throws IOException{
			FileSystem hdfs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(inputFilePath))));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(outputFilePath))));
			
			String line;
			int lineNumber = 0;
			
			while((line = br.readLine()) != null) {
				String[] arrPropPoint = line.split(",");
				PointWritable pointInput = new PointWritable(arrPropPoint);
				
				double minDistance = Double.MAX_VALUE;
				int centroidIdNearest = -1;
				for(int i=0;i<centroids.length;i++) {
					double distance = pointInput.calcDistance(centroids[i]);
					if(distance < minDistance) {
						centroidIdNearest = i;
						minDistance = distance;
					}
				}
				bw.write("Line Number: " +lineNumber + "\t" + "Cluster: "+ centroidIdNearest +"\n");
				lineNumber++;
			}
			br.close();
			bw.close();
		}
		public int run(String[] args) throws Exception{
			Configuration conf = getConf();
			String inputFilePath = conf.get("in", null);
			String outputFolderPath = conf.get("out", null);
			String outputFileName = conf.get("result", "MallCustomerClustering.txt");
			
			int nClusters = conf.getInt("k", 3);
			float thresholdStop = conf.getFloat("thresh", 0.001f);
			int numLineOfInputFile = conf.getInt("lines", 0);
			MAX_LOOP = conf.getInt("maxloop", 50);
			int nReduceTask = conf.getInt("NumReduceTask", 1);
			if(inputFilePath == null || outputFolderPath == null || numLineOfInputFile == 0) {
				System.err.printf("Need to repair your config", getClass().getSimpleName());
				ToolRunner.printGenericCommandUsage(System.err);
				return -1;
			}
			System.out.println("---------------INPUT PARAMETERS---------------");
			System.out.println("inputFilePath:" + inputFilePath);
			System.out.println("outputFolderPath:" + outputFolderPath);
			System.out.println("outputFileName:" + outputFileName);
			System.out.println("maxloop:" + MAX_LOOP);
			System.out.println("numLineOfInputFile:" + numLineOfInputFile);
			System.out.println("nClusters:" + nClusters);
			System.out.println("threshold:" + thresholdStop);
			System.out.println("NumReduceTask:" + nReduceTask);
			
			System.out.println("---------------START----------------");
			PointWritable[] oldCentroidPoints = initRandomCentroids(nClusters, numLineOfInputFile, inputFilePath, conf);
			PointWritable[] centroidsInit = copyCentroids(oldCentroidPoints);
			printCentroids(oldCentroidPoints, "init");
			saveCentroidsForShared(conf, oldCentroidPoints);
			int nLoop = 0;
			
			PointWritable[] newCentroidPoints = null;
			long t1 = (new Date()).getTime();
			while(true) {
				nLoop++;
				if(nLoop == MAX_LOOP) {
					break;
				}
				Job job = new Job(conf, "K-Mean");
				job.setJarByClass(Main.class);
				job.setMapperClass(KMapper.class);
				job.setCombinerClass(KCombiner.class);
				job.setReducerClass(KReducer.class);
				job.setMapOutputKeyClass(LongWritable.class);
				job.setMapOutputValueClass(PointWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				
				FileInputFormat.addInputPath(job, new Path(inputFilePath));
				
				FileOutputFormat.setOutputPath(job, new Path(outputFolderPath));
				job.setOutputFormatClass(TextOutputFormat.class);
				job.setNumReduceTasks(nReduceTask);
				boolean ret = job.waitForCompletion(true);
				if(!ret) {
					return -1;
				}
				newCentroidPoints = readCentroidsFromReducerOutput(conf, nClusters, outputFolderPath);
				printCentroids(newCentroidPoints, "new");
				boolean needStop = checkStopKmean(newCentroidPoints, oldCentroidPoints, thresholdStop);
				
				oldCentroidPoints = copyCentroids(newCentroidPoints);
				
				if(needStop) {
					break;
				}else {
					saveCentroidsForShared(conf, newCentroidPoints);
				}
			}
			if(newCentroidPoints != null) {
				System.out.println("----------------FINAL RESULT----------------");
				writeFinalResult(conf, newCentroidPoints, outputFolderPath + "/" + outputFileName, centroidsInit);
				
				String assignmentOutputPath = outputFolderPath + "/ClusteringResult.txt";
				assignPointsToCentroids(conf, inputFilePath, newCentroidPoints, assignmentOutputPath);
			}
			System.out.println("-----------------------");
			System.out.println("K-MEANS CLUSTERING FINISHED!");
			System.out.println("Loop: "+ nLoop);
			System.out.println("Time: "+((new Date()).getTime() - t1)+"ms");
			
			return 1;
		}
		public static void main(String[] args) throws Exception{
			int exitCode = ToolRunner.run(new Main(), args);
			System.exit(exitCode);
		}
}
