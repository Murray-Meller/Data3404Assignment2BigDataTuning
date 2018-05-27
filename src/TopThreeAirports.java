
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;

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

		// OVERVIEW:
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
		DataSet<Tuple1<String>> filtered = rawData
				.filter(new FilterFlightsByYear()) //returns flights in the given year that actually departed 
				.project(1);
		
		filtered.print(); 
		
		// group according to airport name and orderby count
		filtered.groupBy(0)
			.reduceGroup(new AirportCount())
			.sortPartition(1, Order.DESCENDING)
			.writeAsText("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/mmel5239/data3404/results/TopThreeAirportsResult.txt");// 
		
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

	/**
	 * A GroupReduceFucntion implementation that groups the input by the first field.
	 * Input Tuple1<String : Airport code>
	 * @author murraymeller
	 *
	 */
	public static class AirportCount implements GroupReduceFunction<Tuple1<String>, Tuple2<String, Integer>> {

		@Override
		public void reduce(Iterable<Tuple1<String>> records, Collector<Tuple2<String, Integer>> out) throws Exception {

			String airport = null;
			int cnt = 0;

			for (Tuple1<String> m : records) {
				airport = m.f0;
				cnt++;

			}
			out.collect(new Tuple2<>(airport, cnt));
		}
	}
}
