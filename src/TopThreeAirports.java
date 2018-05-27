
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.operators.Order;

import java.util.Scanner;

public class TopThreeAirports {
	public static void main(String[] args) throws Exception {
		
		System.out.println("The following program will find the top three airports based on the number of departures form that airport for a given year.");
		System.out.println("Please enter the year (yy): ");
		Scanner input = new Scanner(System.in);
		
		while (!input.hasNextInt()) {
			System.out.println("The value needs to be a two digit integer. Please enter the year (yy): ");
			input.next();
		}
		
		int year = input.nextInt();
		FilterFlightsByYear.year = year;
		input.close();
		
		
		// obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//OVERVIEW:
		// need top three airports based on the number of departures from that airport. for a given year
		// rank based on top three
		// need the following fields: date, origin, actual departure (to tell if the flight actually happened)
		// filter out flights that never happened
		// filter out flights not in the correct year
		
		// read raw CSV with data, airport and departure time
		// date, origin, departure
		DataSet<Tuple3<String, String, String>> rawData = env
				.readCsvFile("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/mmel5239/data3404/assesmentDataSet/ontimeperformance_flights_tiny.csv")
				.includeFields("000110000100")
				.ignoreFirstLine()
				.ignoreInvalidLines()
				.types(String.class, String.class, String.class);
		
		// filter flights that aren't in the correct year or never departed
		DataSet<Tuple2<String, String>> filtered = rawData
				.filter(new FilterFlightsByYear())
				.project(1);
		
		filtered.print(); 
		
		filtered.groupBy(0);
		
		
		
		
		// OLD PROGRAM FROM TUTORIAL _ USING IT AS A REFERENCE
		// Define a data set from the users.csv file , include the required
		// fields for the task, in this case user id and region id
//		DataSet<Tuple2<Integer, Integer>> users = env
//				.readCsvFile("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/mmel5239/data3404/assesmentDataSet/ontimeperformance_flights_tiny.csv")
//				.includeFields("1000000001")
//				.ignoreFirstLine()
//				.ignoreInvalidLines()
//				.types(Integer.class, Integer.class); 
//		
//		// Define a data set from the regions.csv file , include the requ ired
//		// fields for the task, in this case region id and region name
//		DataSet<Tuple2<Integer, String>> regions = env
//				.readCsvFile("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/auction_db/users.csv")
//				.includeFields("11")
//				.ignoreFirstLine()
//				.ignoreInvalidLines()
//				.types(Integer.class, String.class);
//
//		// Join to data sets to create a new one tuple data set with region name
//		DataSet<Tuple1<String>> result = users
//				.join(regions)
//				.where(1)
//				.equalTo(0) // joining two data sets using the equality condition U.rid=R.id
//				.with(new JoinUR()); // using a new join function to create the tuple
//		
//		result.groupBy(0)// group according to region name
//				.reduceGroup(new RCounter())
//				.sortPartition(1, Order.DESCENDING)
//				.writeAsText("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/mmel5239/test.txt");// counting the number of users per region and placing top regions first
//		
		env.execute("Executing program");

	}
	
	/**
	 * Filters the flight by the year and removes flights that never left
	 * input: FlightDate, Airport code, departure time
	 * NB. the static variable year must be set externally.
	 * @author murraymeller
	 *
	 */
	private static class FilterFlightsByYear implements FilterFunction<Tuple3<String, String, String>> {
		public static int year = 0;
		
		@Override
		public boolean filter(Tuple3<String, String, String> row) {
			int rowYear = Integer.parseInt( row.f0.substring(row.f0.length() - 3) );
			return rowYear == year || !row.f0.equals("");
		}
	}
	

//	private static class JoinUR
//			implements JoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, String>, Tuple1<String>> {
//
//		/**
//		 * 
//		 */
//		private static final long serialVersionUID = 1L;
//
//		@Override
//		public Tuple1<String> join(Tuple2<Integer, Integer> users, Tuple2<Integer, String> region) {
//			return new Tuple1<>(region.f1);
//
//		}
//
//	}
//
//	public static class RCounter implements GroupReduceFunction<Tuple1<String>, Tuple2<String, Integer>> {
//		/**
//		 * 
//		 */
//		private static final long serialVersionUID = 1L;
//
//		@Override
//		public void reduce(Iterable<Tuple1<String>> records, Collector<Tuple2<String, Integer>> out) throws Exception {
//
//			String region = null;
//			int cnt = 0;
//
//			for (Tuple1<String> m : records) {
//				region = m.f0;
//				cnt++;
//
//			}
//			out.collect(new Tuple2<>(region, cnt));
//		}
//	}
}
