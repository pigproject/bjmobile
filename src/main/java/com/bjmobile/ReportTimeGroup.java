package com.bjmobile;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BinSedesTupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: qeekey
 * Date: 14-6-11
 * Time: 下午7:30
 * To change this template use File | Settings | File Templates.
 */
public class ReportTimeGroup extends EvalFunc<DataBag> {
	String pre = "2020-01-01 00:00:00";
	String date = "2020-01-01 00:00:00";

	@Override
	public DataBag exec(Tuple input) throws IOException {
		DataBag result = DefaultBagFactory.getInstance().newDefaultBag();

		DataBag bag = (DataBag) input.get(0);
		Iterator<Tuple> it = bag.iterator();
		String report_time = date;
		while (it.hasNext()) {
			Tuple new_tuple = BinSedesTupleFactory.getInstance().newTuple();

			Tuple item = it.next();
			report_time = (String) item.get(2);
			String rtt_report_time = (String) item.get(3);
			String B_CELL = (String) item.get(4);
			String B_DX_CAUSE = (String) item.get(5);

			new_tuple.append(report_time);
			new_tuple.append(rtt_report_time);
			new_tuple.append(B_CELL);
			new_tuple.append(B_DX_CAUSE);


			if (pre.compareTo(report_time) != 0) {
				date = pre;
				pre = report_time;
			}

			if (rtt_report_time.compareTo(report_time)>=0){
				new_tuple.append(date);
			}else{
				new_tuple.append("2000-01-01 00:00:00.000000000");
			}

			if (rtt_report_time.compareTo(date)<=0 && rtt_report_time.compareTo(report_time)>=0 ){
				new_tuple.append(1);    //有效数据
			}else{
				new_tuple.append(0);    //无效数据
			}
			result.add(new_tuple);
		}
		return result;
	}

	public static void main(String[] args) throws Exception {
		//1400061490
		Tuple test = BinSedesTupleFactory.getInstance().newTuple();
		String time1 = "2014-05-29 11:00:20.060000000";
		String time2 = "2014-05-29 11:00:10.060000000";
		test.append(time1);
		test.append(time2);

		ReportTimeGroup t = new ReportTimeGroup();
		System.out.println(t.exec(test));
	}

}
