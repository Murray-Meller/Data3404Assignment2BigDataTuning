
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.operators.Order;

public class TopThreeAirports {
	public static void main(String[] args) throws Exception {
		System.out.println("STARTING :D ");
		// obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Define a data set from the users.csv file , include the required
		// fields for the task, in this case user id and region id
		DataSet<Tuple2<Integer, Integer>> users = env
				.readCsvFile("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/auction_db/users.csv")
				.includeFields("1000000001")
				.ignoreFirstLine()
				.ignoreInvalidLines()
				.types(Integer.class, Integer.class);
		
		// Define a data set from the regions.csv file , include the requ ired
		// fields for the task, in this case region id and region name
		DataSet<Tuple2<Integer, String>> regions = env
				.readCsvFile("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/auction_db/users.csv")
				.includeFields("11")
				.ignoreFirstLine()
				.ignoreInvalidLines()
				.types(Integer.class, String.class);

		// Join to data sets to create a new one tuple data set with region name
		DataSet<Tuple1<String>> result = users
				.join(regions)
				.where(1)
				.equalTo(0) // joining two data sets using the equality condition U.rid=R.id
				.with(new JoinUR()); // using a new join function to create the tuple
		
		result.groupBy(0)// group according to region name
				.reduceGroup(new RCounter())
				.sortPartition(1, Order.DESCENDING)
				.writeAsText("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/mmel5239/test.txt");// counting the number of users per region and placing top regions first
		
		env.execute("Executing program");

	}

	private static class JoinUR
			implements JoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, String>, Tuple1<String>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple1<String> join(Tuple2<Integer, Integer> users, Tuple2<Integer, String> region) {
			return new Tuple1<>(region.f1);

		}

	}

	public static class RCounter implements GroupReduceFunction<Tuple1<String>, Tuple2<String, Integer>> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<Tuple1<String>> records, Collector<Tuple2<String, Integer>> out) throws Exception {

			String region = null;

			int cnt = 0;

			for (Tuple1<String> m : records) {

				region = m.f0;

				cnt++;

			}
			out.collect(new Tuple2<>(region, cnt));
		}
	}
}
