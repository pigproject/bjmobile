package com.bjmobile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: qeekey
 * Date: 14-6-11
 * Time: 下午10:26
 * To change this template use File | Settings | File Templates.
 */
public class BjSelectField {
	private static boolean  newFlag = false ;
	private static StringBuffer buf = new StringBuffer();
	private static List<String> fileList = new ArrayList<String>();

	private static  String CELL_CI_VALUE = "";
	private static  String LAC_VALUE = "";
	private static  String TMSI_P_TMSI_VALUE = "";
	private static  String CSMO_VALUE = "";
	private static  String CSMT_VALUE = "";

	private static int pos1 = -1 ;
	private static int pos2 = -1 ;

	private static boolean flag1 = true ;
	private static boolean flag2 = true ;


	enum IM {
		increment, merge
	}

	enum MyCounter{
		head_start1_count1,
		head_start1_count2,
		csmt,
		pos1_csmt,
		pos1_csmt_false,
		pos12_csmt
	}

	static class SelectFieldMapper extends Mapper<LongWritable, Text,  Text, Text> {
		public void map(LongWritable key , Text values , Context context) throws IOException,InterruptedException{
			String line = values.toString();
//			System.out.println("line num=" + BjComm.line_num  + "=" + line + "#");

			pos1 = line.indexOf(BjComm.head_start1);
			if (pos1 !=-1 ){
//				context.getCounter(MyCounter.head_start1_count1).increment(1);
				flag1 = true ;
			}

			pos2 = line.indexOf(BjComm.head_start2);
			if (pos2 !=-1 ){
//				context.getCounter(MyCounter.head_start1_count2).increment(1);
				flag2 = true ;
			}

			int pos_CELL_CI =  line.indexOf(BjComm.CELL_CI) ;
			if ( pos_CELL_CI !=-1 ){
//				System.out.println("##CELL_CI line=" + line);
				CELL_CI_VALUE = line.substring(pos_CELL_CI + BjComm.CELL_CI.length());
//				System.out.println("##CELL_CI_VALUE=" + CELL_CI_VALUE);
			}

			int pos_LAC=  line.indexOf(BjComm.LAC) ;
			if (pos_LAC!=-1){
//				System.out.println("##LAC Line=" + line);
				LAC_VALUE = line.substring(pos_LAC + BjComm.LAC.length());
//				System.out.println("##LAC_VALUE=" + LAC_VALUE);
			}

			int pos_TMSI_P_TMSI=  line.indexOf(BjComm.TMSI_P_TMSI) ;
			if (pos_TMSI_P_TMSI!=-1){
//				System.out.println("##TMSI_P_TMSI line=" + line);
				TMSI_P_TMSI_VALUE = line.substring(pos_TMSI_P_TMSI + BjComm.TMSI_P_TMSI.length());
//				System.out.println("##TMSI_P_TMSI=" + TMSI_P_TMSI_VALUE);
			}


			int pos_CSMO=  line.indexOf(BjComm.CSMO) ;
			if (pos_CSMO!=-1){
//				System.out.println("##CSMO line=" + line);
				CSMO_VALUE = line.substring(pos_CSMO + BjComm.CSMO.length());
//				System.out.println("##CSMO_VALUE=" + CSMO_VALUE);
			}

			int pos_CSMT=  line.indexOf(BjComm.CSMT) ;
			if (pos_CSMT != -1){
//				System.out.println("##CSMT line=" + line);
				CSMT_VALUE = line.substring(pos_CSMT + BjComm.CSMT.length());
//				System.out.println("##CSMT_VALUE=" + CSMT_VALUE);
//				context.getCounter(MyCounter.csmt).increment(1);

				if (flag1){
					boolean validate = true;

					if (CSMO_VALUE.length()==0){
						validate = false;
					}
					if (CSMT_VALUE.length()==0){
						validate = false;
					}
					if (TMSI_P_TMSI_VALUE.length()==0){
						validate = false;
					}
					if (LAC_VALUE.length()==0){
						validate = false;
					}
					if (CELL_CI_VALUE.length()==0){
						validate = false;
					}

					buf.append(CSMO_VALUE).append(BjComm.split_str).
						append(CSMT_VALUE).append(BjComm.split_str).
						append(TMSI_P_TMSI_VALUE).append(BjComm.split_str).
						append(LAC_VALUE).append(BjComm.split_str).
						append(CELL_CI_VALUE);

					String result = buf.toString().substring(0,buf.toString().length());
					context.getCounter(MyCounter.pos1_csmt).increment(1);

					boolean validateFlag = true;

					String checkstr[] = result.split(BjComm.split_str);
					int flen = checkstr.length;
					for (int i=0;i<flen;i++){
						if (checkstr[i].length() == 0){
							validateFlag = false;
							break;
						}
					}

   	                //全部有值的数据,才输出
					if (validate){
						context.write(new Text(""), new Text(result));
					}

					pos1 = -1 ;
					pos2 = -1 ;

					flag1 = false;
					flag2 = false;

					CELL_CI_VALUE = "";
					LAC_VALUE = "";
					TMSI_P_TMSI_VALUE = "";
					CSMO_VALUE = "";
					CSMT_VALUE ="";

					buf.setLength(0);
				}else{
//					context.getCounter(MyCounter.pos1_csmt_false).increment(1);
				}

			}

		}
	}

//	static class SelectFieldReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
//		public void reduce(LongWritable key , Iterable<Text> values , Context context) throws IOException,InterruptedException{
//			String Value = "";
//			for (Text value:values){
//				Value = value.toString();
//			}
//			context.write(key, new Text(Value));
//		}
//	}

	public static void main(String[] args) throws Exception{
		if (args.length !=2 ){
			System.err.println("Usage: hadoop jar com.zqk.BjSelectField <input path> <output path>");
			System.exit(-1);
		}

		for (int i=0;i<5;i++){
			fileList.add("");
		}

		Configuration conf = new Configuration();
//		conf.set("mapred.line.input.format.linespermap", "189");

		Job job = new Job(conf, "BjSelectField");

		job.setJarByClass(BjSelectField.class);
		job.setJobName("db_115846_91228_DIST_2014-06-12_16-44-01");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

//		使用自定义MAP分割类
// 		job.setInputFormatClass(BjLineInputFormat.class);
//		job.setInputFormatClass(NLineInputFormat.class);

		job.setMapperClass(SelectFieldMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true)?0:1);
	}

}
