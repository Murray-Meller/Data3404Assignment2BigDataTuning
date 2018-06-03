
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import java.util.Scanner;

public class TestingFilter {
	public static void main(String[] args) throws Exception {
		// Get teh user to specify the year they are interested in.
		System.out.println("The following program will find the top three airports based on the number of departures form that airport for a given year.");
		System.out.println("Please enter the year (yy): ");
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
		
		DataSet<Tuple1<String>> rawData = env
				.readCsvFile("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_flights_tiny.csv")
				.includeFields("000100000000")
				.ignoreFirstLine()
				.ignoreInvalidLines()
				.types(String.class);		
		
		rawData.map(new MapFlights()).filter(new FilterFlightsByYear(year)).writeAsCsv("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/mmel5239/TestingDates-" + year + ".txt", "\n", "\t", WriteMode.OVERWRITE).setParallelism(1);;
		
		env.execute("Executing program");

	}
	
	public static class MapFlights implements MapFunction<Tuple1<String>, Tuple2<String, Integer>> {
		@Override
		public Tuple2<String, Integer> map(Tuple1<String> in) {
			String date = in.f0;
			String flightYear = null;
			flightYear = (date.length() >= 4) ? date.substring(0,4) : null;

			
			if (flightYear == null) {
				System.out.println("FAIL");
			}
			else{
				try {
					int rowYear = Integer.parseInt(flightYear);
					return new Tuple2<String, Integer>(in.f0, rowYear);
				}
				catch (Exception e) {
					System.out.println("FAIL");
				}
			}
			return null;
		}
	}
	
	/**
	 * Filters the flight by the year and removes flights that never departed
	 * input: Tuple3<String: FlightDate, String: Airport code, String: departure time>
	 * NB. the static variable year must be set externally.
	 * @author murraymeller
	 */
	public static class FilterFlightsByYear implements FilterFunction<Tuple2<String, Integer>> {
		int year;
		
		public FilterFlightsByYear(int year) {
			this.year = year;
		}
		
		@Override
		public boolean filter(Tuple2<String, Integer> row) {
			return row.f1 == this.year; 
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
