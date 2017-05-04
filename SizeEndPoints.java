import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

public class SizeEndPoints {

	private static Map<String, String> mErrors = new HashMap<String, String>();
	private static Map<String, String> mURLRedirected = new HashMap<String, String>();
	private static Map<String, Integer> sizes = new HashMap<String, Integer>();
	private static Set<String> setGoodEndPoints = new HashSet<String>();
	public static void main(String[] args) throws IOException {
		//analyseLog();
		long start = System.currentTimeMillis();
		System.out.println("Getting size EndPoints");
		getSizeEachEndPoint();
		System.out.println("Getting size EndPoints with errors/Redirection");
		getSizeEndPointErrors();
		long total = System.currentTimeMillis() - start;
		System.out.println("Total time: = "+total+ "\nWriting files.");
		generateFile(sizes,"EndPointSizes.csv");
		generateFile(mErrors, "EndPointSizesError.csv", true);
		generateFile(mURLRedirected, "EndPointSizesRedirect.csv", true);
	}

	
	public static void getSizeEachEndPoint(){
		setGoodEndPoints = QueryEndPoints.getGoodEndPoints();
		for (String endPoint : setGoodEndPoints) {
			int size = getSize(endPoint);
			sizes.put(endPoint, size);
		}
	}
	
	public static void getSizeEndPointErrors(){
		mErrors.forEach((endPoint, error) ->{		
			int size;
			try {
				String urlRedirect = QueryEndPoints.getRedirection(endPoint);
				size = getSize(urlRedirect);
				if(size > 0){ 
					sizes.put(urlRedirect, size);
					mURLRedirected.put(endPoint, urlRedirect);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		});
	}

	private static int getSize(String endPoint) {
		int result = 0;
		String sparqlQueryString = "select (count(*) as ?c) where { ?s ?p ?o }";

		Query query = QueryFactory.create(sparqlQueryString);

		QueryExecution qexec = null;
		try{
			qexec = QueryExecutionFactory.sparqlService(endPoint, query);
		}catch(Exception e){
			mErrors.put(endPoint, e.getMessage());
			return result;
		}
			
		try {
			ResultSet results = qexec.execSelect();
			for (; results.hasNext();) {
				try {
					QuerySolution soln = results.nextSolution();
					result = soln.getLiteral("?c").getInt();
				} catch (Exception e) {
				}
				// result.add(soln.get("?strLabel").toString());
				// System.out.println(soln.get("?strLabel"));
			}
		} catch (Exception e) {
			mErrors .put(endPoint, e.getMessage());
			return result;
		}

		finally {
			qexec.close();
		}
		return result;
	}

	public static File generateFile(Map<String, Integer> endPointSize, String fileName) {
		File ret = new File(fileName);
		try {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			endPointSize.forEach((endPoint, size) -> {
				writer.println(endPoint + "\t" + size);
			});
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret;
	}
	
	public static File generateFile(Map<String, String> endPointSize, String fileName, boolean p) {
		File ret = new File(fileName);
		try {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			endPointSize.forEach((endPoint, error) -> {
				writer.println(endPoint + "\t" + error);
			});
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret;
	}
}
