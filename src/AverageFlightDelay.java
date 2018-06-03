
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import java.util.Scanner;

public class AverageFlightDelay {
	public static void main(String[] args) throws Exception {
		
		// Get the user to specify the year they are interested in.
		System.out.println("The following program will find the average delay for flights within each airline for a given year.");
		System.out.println("Please enter the year (yyyy): ");
		Scanner input = new Scanner(System.in);
		
		while (!input.hasNextInt()) {
			System.out.println("The value needs to be a two digit integer. Please enter the year (yyyy): ");
			input.next();
		}
		
		int year = input.nextInt();
		input.close();
		
		// Obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//	OVERVIEW
		//		read in flights CSV getting needed columns as
		// 			TUPLE6<String: carrier code, String: date, String: schedule depart, String: schedule arrive, String: actual depart, String: actual arrive>
		//			filter out flights that aren't in the right year (ignores flights that were registered but never departed)
		//		 	group by carrier code
		// 			group reduce to TUPLE3<String: carrier code, Integer: SUM, Integer: COUNT> // could optimse and go stragiht to average
		//		read in airline CSV as TUPLE2<String: carrier code, String: name>
		//		join two datasets on carrier code
		// 		map resulting dataset to TUPLE2<String: Airline name, Integer: code>
			
		
		// vvvvvvvvvv DONE IN PARALLEL THANKS TO FLINK vvvvvvvvvv
		
		DataSet<Tuple6<String, String, String, String, String, String>> flights = env
				.readCsvFile("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_flights_large.csv")
				.includeFields("010100011110")
				.ignoreFirstLine()
				.ignoreInvalidLines()
				.types(String.class, String.class, String.class, String.class, String.class, String.class)
				.filter(new FilterFlightsByYear(year)); //returns flights in the given year that actually departed	
		
		DataSet<Tuple2<String, String>> airlines = env
				.readCsvFile("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_airlines.csv")
				.includeFields("1100")
				.ignoreFirstLine()
				.ignoreInvalidLines()
				.types(String.class, String.class);
		
		DataSet<Tuple2<String, Integer>> averageDelay = flights
				.groupBy("f0") // group by carrier code
				.reduceGroup(new AverageAirlineDelay());
		
		
		DataSet<Tuple2<String, Integer>> joined = airlines.join(averageDelay)
				.where("f0")
				.equalTo("f0")
				.with(new JoinFlightWithAirline());
				
		
		// ^^^^^^^^^^ DONE IN PARALLEL THANKS TO FLINK ^^^^^^^^^^
		
		// write the top three results to disk (note: list is sorted, so top three items = top three airports)
		joined.sortPartition(0, Order.ASCENDING).setParallelism(1).writeAsCsv("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/mmel5239/AverageFlightDelayPerAirline-" + year + ".txt", "\n", "\t", WriteMode.OVERWRITE).setParallelism(1);
		
		env.execute("Executing program");
	}
	
	/**
	 * Filters the flight by the year and removes flights that never departed
	 * input: TUPLE6<String: carrier code, String: date, String: schedule depart, String: schedule arrive, String: actual depart, String: actual arrive>
	 * NB. the static variable year must be set externally.
	 * @author murraymeller
	 */
	public static class FilterFlightsByYear implements FilterFunction<Tuple6<String, String, String, String, String, String>> {
		int year;
		
		public FilterFlightsByYear(int year) {
			this.year = year;
		}
		
		@Override
		public boolean filter(Tuple6<String, String, String, String, String, String> row) {
			String date = row.f1;
			String depart = row.f4;
			
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
	 * A GroupReduceFucntion implementation that calculates the average delay of each airline
	 * Input : TUPLE6<String: carrier code, String: date, String: schedule depart, String: schedule arrive, String: actual depart, String: actual arrive>
	 * Output: Tuple2<String: Airport code, Integer: Average delay in minutes>
	 * @author murraymeller
	 */
	public static class AverageAirlineDelay implements GroupReduceFunction<Tuple6<String, String, String, String, String, String>, Tuple2<String, Integer>> {

		@Override
		public void reduce(Iterable<Tuple6<String, String, String, String, String, String>> records, Collector<Tuple2<String, Integer>> out) throws Exception {
			int sumDelay = 0;
			int count = 0;
			String code = "";
			for (Tuple6<String,String,String,String,String,String> flight : records) {
				code = flight.f0;
				// calculate delay
				int scheduleDepart = convertStringToMinutes(flight.f2);
				int scheduleArrive = convertStringToMinutes(flight.f3);
				int actualDepart = convertStringToMinutes(flight.f4);
				int actualArrive = convertStringToMinutes(flight.f5);
				
				int arriveDelay = actualArrive - scheduleArrive;
				int departDelay = actualDepart - scheduleDepart;
				
				if (arriveDelay > 0) {
					sumDelay += arriveDelay;
				}
				if (departDelay > 0) {
					sumDelay += departDelay;
				}
				count++; 
			}
			out.collect(new Tuple2<String, Integer>(code, sumDelay / count)); // TODO: fix
		}
		
		private int convertStringToMinutes(String in) {
			String tokens[] = in.split(":");
			if (tokens.length != 3) {
				return 0;
			}
			return Integer.parseInt(tokens[0]) * 60 + Integer.parseInt(tokens[1]);
		}
	}
	
	/**
	 * in1: TUPLE2<String: carrier code, String: airline name>
	 * in2: TUPLE2<String: carrier code, Integer: Avg Delay in minutes>
	 * out: TUPLE2<String: airline name, Integer: Avg delay in minutes
	 * @author murraymeller
	 *
	 */
	public static class JoinFlightWithAirline implements JoinFunction<Tuple2<String, String>, Tuple2<String, Integer>, Tuple2<String, Integer>> {
		
		@Override
		public Tuple2<String, Integer> join(Tuple2<String, String> airline, Tuple2<String, Integer> delay) {
			// multiply the points and rating and construct a new output tuple
			return new Tuple2<String, Integer>(airline.f1, delay.f1);
		}
	}

}
