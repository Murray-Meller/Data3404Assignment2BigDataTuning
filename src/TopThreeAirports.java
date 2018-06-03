
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import java.util.Scanner;

public class TopThreeAirports {
	public static void main(String[] args) throws Exception {
		// Get the user to specify the year they are interested in.
		System.out.println("The following program will find the top three airports based on the number of departures form that airport for a given year.");
		System.out.println("Please enter the year (yyyy): ");
		Scanner input = new Scanner(System.in);
		
		while (!input.hasNextInt()) {
			System.out.println("The value needs to be a two digit integer. Please enter the year (yy): ");
			input.next();
		}
		
		int year = input.nextInt();
		input.close();
		System.out.println("Entered Year: " + year);
		
		// Obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// OVERVIEW:
			// need top three airports based on the number of departures from that airport. for a given year
			// rank based on top three
			// need the following fields: date, origin, actual departure (to tell if the flight actually happened)
			// filter out flights that never happened
			// filter out flights not in the correct year
			// count # of instances for airport
			// merge counts globally, then print.
		
		// vvvvvvvvvv DONE IN PARALLEL THANKS TO FLINK vvvvvvvvvv
		
		// read raw CSV with data, airport and departure time
		// date, origin, departure
		// This contains optimisations in that it only selects the attributes necessary to the search. I.e. we dont read the whole table in
		DataSet<Tuple3<String, String, String>> rawData = env
				.readCsvFile("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_flights_tiny.csv")
				.includeFields("000110000100")
				.ignoreFirstLine()
				.ignoreInvalidLines()
				.types(String.class, String.class, String.class);		

		// filter flights that aren't in the correct year or never departed
		// OPT: reduces result size. 
		DataSet<Tuple1<String>> filtered = rawData
			.filter(new FilterFlightsByYear(year)) //returns flights in the given year that actually departed 
			.project(1);	

		//sort results
		DataSet<Tuple2<String, Integer>> sorted = filtered.groupBy(0)
				.reduceGroup(new AirportCount())
				.sortPartition(1, Order.DESCENDING).setParallelism(1);
		
		// ^^^^^^^^^^ DONE IN PARALLEL THANKS TO FLINK ^^^^^^^^^^
		
		// write the top three results to disk (note: list is sorted, so top three items = top three airports)
		sorted.first(3).writeAsCsv("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/mmel5239/TopThreeAirportsFinal-" + year + ".txt", "\n", "\t", WriteMode.OVERWRITE).setParallelism(1);
		
		env.execute("Executing program");

	}
	
	/**
	 * Filters the flight by the year and removes flights that never departed
	 * input: Tuple3<String: FlightDate, String: Airport code, String: departure time>
	 * NB. the static variable year must be set externally.
	 * @author murraymeller
	 */
	private static class FilterFlightsByYear implements FilterFunction<Tuple3<String, String, String>> {
		int year;
		
		public FilterFlightsByYear(int year) {
			this.year = year;
		}
		
		@Override
		public boolean filter(Tuple3<String, String, String> row) {
			String date = row.f0;
			String depart = row.f2;
			
			if ((date == null || depart == null) || row.f2.length() <= 0) {
				return false;
			}

			String flightYear = null;
			flightYear = (date.length() >= 4) ? date.substring(0,4) : null;

			
			if (flightYear == null) {
				return false;
			}
			else{
				try {
					int rowYear = Integer.parseInt(flightYear);
					return rowYear == this.year;
				}
				catch (Exception e) {
					return false;
				}
			}
		}
	}
	
	/**
	 * A GroupReduceFucntion implementation that groups airport by their code and counts them
	 * 
	 * Input : Tuple1<String: Airport code>
	 * Output: Tuple2<String: Airport code, Integer: # of departures>
	 * @author murraymeller
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
