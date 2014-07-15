package com.bjmobile;

import com.bjmobile.utils.DateUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BinSedesTupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: qeekey
 * Date: 14-6-11
 * Time: 下午7:30
 * To change this template use File | Settings | File Templates.
 */
public class ReportTime10S extends EvalFunc<Integer> {


	@Override
	public Integer exec(Tuple input) throws IOException {
		if (input == null | input.size() == 0) {
			return null;
		}
		int flag = 0 ;

		String  report_time = (String) input.get(0);
		String  rtt_report_time = (String) input.get(1);

		report_time.substring(0,19);
		rtt_report_time = rtt_report_time.substring(0,19);

		Date report_time1 = DateUtils.StrToDate(report_time,DateUtils.FORMAT_DAY_HMS);
		Date report_time2 = DateUtils.StrToDate(rtt_report_time,DateUtils.FORMAT_DAY_HMS);

		long  report_time1_long = report_time1.getTime();
		long  report_time1_10s_long = report_time1.getTime() + 10000 ; //10秒后的时间

		long  report_time2_long = report_time2.getTime();

		if ( (report_time2_long >= report_time1_long) && (report_time2_long<=report_time1_10s_long)  ) {
			return 1;
		}else {
			return 0;
		}

	}




	public static void main(String[] args) throws Exception {
		//1400061490
		Tuple test = BinSedesTupleFactory.getInstance().newTuple();
		String time1 = "2014-05-29 11:00:20.060000000";
		String time2 = "2014-05-29 11:00:10.060000000";
		test.append(time1);
		test.append(time2);

		ReportTime10S t = new ReportTime10S();
		System.out.println(t.exec(test));
	}

}
