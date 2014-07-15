package com.bjmobile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;


public class BjLineInputFormat extends FileInputFormat<LongWritable, Text> {
	public static final String LINES_PER_MAP = "mapreduce.input.lineinputformat.linespermap";

	private static boolean head_flag = false ;

	private static StringBuffer buf = new StringBuffer();

	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		context.setStatus(genericSplit.toString());
		return new LineRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
//		return super.isSplitable(context, filename);    //To change body of overridden methods use File | Settings | File Templates.
	}

	/**
	 * Logically splits the set of input files for the job, splits N lines
	 * of the input as one split.
	 * @see FileInputFormat#getSplits(JobContext)
	 */
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		int numLinesPerSplit = getNumLinesPerSplit(job);
		for (FileStatus status : listStatus(job)) {
			splits.addAll(getSplitsForFile(status, job.getConfiguration(), numLinesPerSplit));
		}
		return splits;
	}

	public static List<FileSplit> getSplitsForFile(FileStatus status,Configuration conf, int numLinesPerSplit) throws IOException {
		List<FileSplit> splits = new ArrayList<FileSplit>();
		Path fileName = status.getPath();
		if (status.isDir()) {
			throw new IOException("Not a file: " + fileName);
		}
		FileSystem fs = fileName.getFileSystem(conf);
		LineReader lr = null;
		try {
			FSDataInputStream in = fs.open(fileName);
			lr = new LineReader(in, conf);
			Text line = new Text();
			int numLines = 0;
			long begin = 0;
			long length = 0;
			int num = -1;

			int pos1 = 0 ;
			int pos2 = 0 ;

			int line_id = 0 ;
			int line_id_head1 = 0 ;
			int line_id_head2 = 0 ;

			while ((num = lr.readLine(line)) > 0) {
				numLines++;
				length += num;

				line_id ++ ;
//				System.out.println("BjLineInputFormat line_id=" + line_id  + ",line=" + line + "#");

				pos1 = line.toString().indexOf(BjComm.head_start1);
				if (pos1 !=-1){
					head_flag = true;

					buf.append("line_id_head1="+ line_id ).append(",");

					System.out.println("\n" );
					System.out.println("\n" );
					System.out.println("1 buf="+ buf.toString() );
					System.out.println("\n" );
					System.out.println("\n" );
				}

				/*
				pos2 = line.toString().indexOf(BjComm.head_start2);
				if (pos2 !=-1){
					head_flag2 = true;

					buf.append("-line_id_head2="+ line_id ).append(",");

					System.out.println("\n" );
					System.out.println("\n" );
					System.out.println("2 buf="+ buf.toString() );
					System.out.println("\n" );
					System.out.println("\n" );
				}
				*/

//				if (numLines == numLinesPerSplit) {
//				if (pos1 == 0 && pos2==0) {
				if (head_flag) {
					// NLineInputFormat uses LineRecordReader, which always reads
					// (and consumes) at least one character out of its upper split
					// boundary. So to make sure that each mapper gets N lines, we
					// move back the upper split limits of each split
					// by one character here.
					if (begin == 0) {
						splits.add(new FileSplit(fileName, begin, length - 1, new String[]{}));
					} else {
						splits.add(new FileSplit(fileName, begin - 1, length, new String[]{}));
					}
					begin += length;
					length = 0;
					numLines = 0;

					buf.setLength(0);

					head_flag = false;

					System.out.println("##splits.size=" + splits.size());


				}
			}

			if (numLines != 0) {
				splits.add(new FileSplit(fileName, begin, length, new String[]{}));
			}

		} finally {
			if (lr != null) {
				lr.close();
			}
		}

//		Iterator it = splits.iterator();
//		while (it.hasNext()){
//			InputSplit is = (InputSplit)it.next();
//			try {
//				System.out.println("##is=" + is.getLength());
//			} catch (InterruptedException e) {
//				e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//			}
//		}

		return splits;
	}

	/**
	 * Set the number of lines per split
	 *
	 * @param job      the job to modify
	 * @param numLines the number of lines per split
	 */
	public static void setNumLinesPerSplit(Job job, int numLines) {
		job.getConfiguration().setInt(LINES_PER_MAP, numLines);
	}

	/**
	 * Get the number of lines per split
	 *
	 * @param job the job
	 * @return the number of lines per split
	 */
	public static int getNumLinesPerSplit(JobContext job) {
		return job.getConfiguration().getInt(LINES_PER_MAP, 1);
	}
}
