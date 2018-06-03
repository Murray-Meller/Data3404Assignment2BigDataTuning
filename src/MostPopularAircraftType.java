
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import java.util.Iterator;

public class MostPopularAircraftType {
	public static void main(String[] args) throws Exception {
		
		// Obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// vvvvvvvvvv DONE IN PARALLEL THANKS TO FLINK vvvvvvvvvv
		
		// Read in flights <Carrier code, TailCode, count>
		DataSet<Tuple3<String, String, Integer>> flights = env
				.readCsvFile("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_flights_large.csv")
				.includeFields("010000100000")
				.ignoreFirstLine()
				.ignoreInvalidLines()
				.types(String.class, String.class) // outputs: flights <Carrier code, TailCode>
				.groupBy(1)
				.reduceGroup(new CountTailcCodesNumberOfFlights()); // outputs: flights <Carrier code, Tailcode, Count>
		
		DataSet<Tuple3<String, String, String>> aircrafts = env
				.readCsvFile("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_aircrafts.csv")
				.includeFields("101010000") // <tail code, manufacturer, model>
				.ignoreFirstLine()
				.ignoreInvalidLines()
				.types(String.class, String.class, String.class);
		
		DataSet<Tuple4<String, String, String, Integer>> topFiveAircraftOfEachAirline = 
				flights.join(aircrafts)
				.where(1)
				.equalTo(0)
				.with(new JoinFlightWithAirline()) // outputs: flights joined aircraft <carriercode, manufacture, model, count>
				.groupBy(0,1,2) // carrier code, manufacturer, model
				.reduceGroup(new CountAircraftsNumberOfFlights()); // output: AircraftCountPerAirline <carrierCode, manufacturer, model, count>
				
		DataSet<Tuple2<String, String>> sortedFlights = topFiveAircraftOfEachAirline
				.groupBy(0)
				.sortGroup(3, Order.DESCENDING)
				.first(5) //outputs: AircraftCountPerAirline <carrierCode, manufacturer, model, count>
				.groupBy(0)
				.sortGroup(3, Order.DESCENDING)
				.reduceGroup(new FormatOutput()); //TODO: should take the first 5 in every group
				
		sortedFlights.writeAsCsv("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/mmel5239/MostPopularAircraftTypeTest.txt", "\n", "\t", WriteMode.OVERWRITE).setParallelism(1);
		
		DataSet<Tuple2<String, String>> airlines = env
				.readCsvFile("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_airlines.csv")
				.includeFields("1100")
				.ignoreFirstLine()
				.ignoreInvalidLines()
				.types(String.class, String.class);
		
		DataSet<Tuple2<String,String>> formattedTopFiveAircraftPerCarrier = 
				sortedFlights.join(airlines)
				.where(0)
				.equalTo(0)
				.with(new JoinTopFiveWithAirlineName()); // output: <Airline name, manufacturer, model, count>  		
		
		// ^^^^^^^^^^ DONE IN PARALLEL THANKS TO FLINK ^^^^^^^^^^
		
		// write the top three results to disk (note: list is sorted, so top three items = top three airports)
		formattedTopFiveAircraftPerCarrier
		.sortPartition(0, Order.ASCENDING).setParallelism(1)
		.writeAsCsv("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/mmel5239/MostPopularAircraftType.txt", "\n", "\t", WriteMode.OVERWRITE).setParallelism(1);
		
		env.execute("Executing program");
	}

	/**
	 * A GroupReduceFucntion implementation that groups flights by their tailcode and the number of flights for that particular plane
	 * 
	 * Input : Tuple2<String: carrier code, String: tailcode>
	 * Output: Tuple3<String: carrier code, String: tailcode, Integer: # of flights>
	 * @author murraymeller
	 */
	public static class CountTailcCodesNumberOfFlights implements GroupReduceFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {

		@Override
		public void reduce(Iterable<Tuple2<String, String>> records, Collector<Tuple3<String, String, Integer>> out) throws Exception {
			String carrierCode = null;
			String tailCode = null;
			int cnt = 0;

			for (Tuple2<String,String> m : records) {
				carrierCode = m.f0;
				tailCode = m.f1;
				cnt++;

			}
			out.collect(new Tuple3<>(carrierCode, tailCode, cnt));
		}
	}

	/**
	 * in1: TUPLE3<String: carrier code, String: tailcode, Integer: count>
	 * in2: TUPLE3<String: TailNumber, String: manufacturer, String: model>
	 * out: TUPLE4<String: carrier, manufacturer, model, count>
	 * @author murraymeller
	 *
	 */
	public static class JoinFlightWithAirline implements JoinFunction<Tuple3<String, String, Integer>, Tuple3<String, String, String>, Tuple4<String, String, String, Integer>> {
		
		@Override
		public Tuple4<String, String, String, Integer> join(Tuple3<String, String, Integer> flight, Tuple3<String, String, String> aircraft) {
			// multiply the points and rating and construct a new output tuple
			return new Tuple4<>(flight.f0, aircraft.f1, aircraft.f2, flight.f2);
		}
	}
	
	/**
	 * A GroupReduceFucntion implementation that groups flights by their manufacturer and model and counts the number of flights for that particular plane
	 * 
	 * Input : Tuple4<String: carrier code, String: manufacturer, String: model, Integer:count>
	 * Output: Tuple4<String: carrier code, String: manufacturer, String: model, Integer:count>
	 * @author murraymeller
	 */
	public static class CountAircraftsNumberOfFlights implements GroupReduceFunction<Tuple4<String, String, String, Integer>, Tuple4<String, String, String, Integer>> {

		@Override
		public void reduce(Iterable<Tuple4<String, String, String, Integer>> planes, Collector<Tuple4<String, String, String, Integer>> out) throws Exception {
			String carrierCode = null;
			String manufacturer = null;
			String model = null;
			int cnt = 0;

			for (Tuple4<String,String, String, Integer> m : planes) {
				carrierCode = m.f0;
				manufacturer = m.f1;
				model = m.f2;
				cnt += m.f3;
			}
			out.collect(new Tuple4<>(carrierCode, manufacturer, model, cnt));
		}
	}
	
	/**
	 * in1: TUPLE2<String: carrier code, String: top five formatted>
	 * in2: TUPLE2<String: carrier code, String: airline name>
	 * out: TUPLE2<String: Airline name, top 5 formatted>
	 * @author murraymeller
	 *
	 */
	public static class JoinTopFiveWithAirlineName implements JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> {
		
		@Override
		public Tuple2<String, String> join(Tuple2<String, String> aircraft, Tuple2<String, String> airline) {
			// multiply the points and rating and construct a new output tuple
			return new Tuple2<>(airline.f1, aircraft.f1);
		}
	}
	
	/**
	 * in: TUPLE4<String: carrier code, STring:manufacturer, String:model, Integer:count>
	 * out: TUPLE2<String: carrier code, String: [Manufacture Model x 5]
	 * @author murraymeller
	 */
	public static class FormatOutput implements GroupReduceFunction<Tuple4<String, String, String, Integer>, Tuple2<String, String>> {

		@Override
		public void reduce(Iterable<Tuple4<String, String, String, Integer>> aircraftStat, Collector<Tuple2<String, String>> out) throws Exception {
			String airlineName = "";
			String aircraftStats = "[";
			
			//Tuple4<String, String, String, Integer> a aircraftStat
			for (Iterator<Tuple4<String, String, String, Integer>> it = aircraftStat.iterator(); it.hasNext(); ) {
				Tuple4<String, String, String, Integer>  a = it.next();
				airlineName = a.f0;
				aircraftStats += a.f1 + " " + a.f2 + " " + a.f3;
				if (it.hasNext()) {
					aircraftStats += ", ";
				}
			}
			aircraftStats += "]";
			out.collect(new Tuple2<>(airlineName, aircraftStats));
		}
	}
	
}
