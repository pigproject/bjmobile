package com.bjmobile;

import com.bjmobile.utils.DateUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BinSedesTupleFactory;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: qeekey
 * Date: 14-6-11
 * Time: 下午7:30
 * To change this template use File | Settings | File Templates.
 */
public class ReportTime extends EvalFunc<Integer> {
	String date = "0000-00-00 00:00:00";

	@Override
	public Integer exec(Tuple input) throws IOException {
		if (input == null | input.size() == 0) {
			return null;
		}
		String  report_time = (String) input.get(0);
		return 1;
	}

	public static void main(String[] args) throws Exception {
		//1400061490
		Tuple test = BinSedesTupleFactory.getInstance().newTuple();
		String time1 = "2014-05-29 11:00:20.060000000";
		String time2 = "2014-05-29 11:00:10.060000000";
		test.append(time1);
		test.append(time2);

		ReportTime t = new ReportTime();
		System.out.println(t.exec(test));
	}

}
