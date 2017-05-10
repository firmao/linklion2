import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CheckingEndpointsLeft {

	public static void main(String[] args) throws IOException {
		analyseLog();
		
		//Set<String> ret = FirstOptimization.getTriples("http://smartcity.linkeddata.es/BECA/sparql/");
		//System.out.println(ret);
	}

	public static void analyseLog() throws IOException{
		File fLog = new File("log.txt");
		int totalTriples = QueryEndPoints.getLogEndpoints(fLog, "TotalNumberOfTriples: ");
		Set<String> setLogEndpoints = QueryEndPoints.getLogEndpoints(fLog);
		Set<String> setGoodEndPoints = QueryEndPoints.getGoodEndPoints();
		Set<String> setLeftEndpoints = new HashSet<String>();
		for (String elem : setGoodEndPoints) {
			if(!setLogEndpoints.contains(elem)){
				setLeftEndpoints.add(elem);
			}
		}
		System.out.println("Good Endpoints from LODStats: " + setGoodEndPoints.size());
		System.out.println("Total triples processed until now: " + totalTriples);
		System.out.println("EndPoints processed (from "+ fLog.getName() +"): " + setLogEndpoints.size());
		System.out.println("Good Left endPoints(Not processed yet): " + setLeftEndpoints.size());
		System.out.println(setLeftEndpoints);
	}
}
