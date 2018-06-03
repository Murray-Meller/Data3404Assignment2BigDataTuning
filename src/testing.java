
public class testing {

	public static void main(String[] argv) {
		String date = "14/10/97";
		String flightYear = null;
		flightYear = (date.length() >= 2) ? date.substring(date.length() - 2) : null;

		
		if (flightYear == null) {
			System.out.println("FAIL");
		}
		else{
			try {
				int rowYear = Integer.parseInt(flightYear);
				System.out.println(rowYear == 97);
			}
			catch (Exception e) {
				System.out.println("FAIL");
			}
		}
	}
	
}
