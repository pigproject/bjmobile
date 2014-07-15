package com.bjmobile;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: qeekey
 * Date: 14-7-8
 * Time: 上午10:31
 * To change this template use File | Settings | File Templates.
 */
public class ReportInterval extends EvalFunc<Integer> {

	@Override
	public Integer exec(Tuple input) throws IOException {
		if (input == null | input.size() == 0) {
			return null;
		}
		Integer ret = 0 ;
		String  report_time_start = (String) input.get(0);
		String  rtt_report_time = (String) input.get(1);
		String  report_time_end = (String) input.get(2);

		if (rtt_report_time.compareTo(report_time_start)>=0){
			if (rtt_report_time.compareTo(report_time_start)<0){
				ret = 1 ;
			}
		}
		return ret;
	}
}
