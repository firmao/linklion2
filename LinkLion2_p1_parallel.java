import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.hp.hpl.jena.ontology.DatatypeProperty;
import com.hp.hpl.jena.ontology.OntDocumentManager;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class LinkLion2_p1_parallel {

	static Map<String, String> mEndPointError = new HashMap<String, String>();
	static boolean DEBUG = true;
	
	public static void main(String[] args) throws IOException {
		/*
		 * For each triple in dump file or Endpoint: If object typeof datatype:
		 * Count +1 for subject URI
		 */
		//if((args.length > 0) && (args[0].equals("false"))) DEBUG=false;
		System.out.println("Debug: " + DEBUG);
		Map<String, String> mSubjEndPoint = new HashMap<String, String>();
		Map<String, Integer> mSubjCount = new HashMap<String, Integer>();
		Set<String> setEndPoints = getEndPoints(DEBUG);
		if(DEBUG)
			System.out.println("Number of endPoints: " + setEndPoints.size());
		long start = System.currentTimeMillis();
		System.out.println("Parallel version:");
		try {
			setEndPoints.parallelStream().forEach(endPoint -> {
			//setEndPoints.forEach(endPoint -> {	
				Set<String> setTriples = getTriples(endPoint);
				Set<String> dTypes = new HashSet<String>();
				//setTriples.parallelStream().forEach(triple -> { // Parallel stuff here brings concurrency problems.
				setTriples.forEach(triple -> {
					String[] sTriple = triple.split("\t", -1);
					String subj = sTriple[0];
					String pred = sTriple[1];
					String obj = sTriple[2];
					//boolean bIsDataType = !isDataType(obj); // Jena.
					//boolean bIsDataType = isDataTypeLiteral(obj); // Andre
					//if (bIsDataType) {
						dTypes.add(obj);
						mSubjEndPoint.put(subj, endPoint);
						if (mSubjCount.containsKey(subj)) {
							Integer value = mSubjCount.get(subj);
							mSubjCount.put(subj, value + 1);
						} else {
							mSubjCount.put(subj, 1);
						}
						// addDB(mSubjCount);
					//}
				});
				if(DEBUG) System.out.println("EndPoint: " + endPoint + "\nTotalNumberOfTriples: " + setTriples.size() + "\nNumDataTypes: " + dTypes.size());
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		long totalTime = System.currentTimeMillis() - start;
		if(DEBUG){ System.out.println("Total time: " + totalTime
				+ " -- Depends on the internet connexion, because need to access the endpoints.");
		System.out.println("Number of Subjects with Objects as DataType: " + mSubjCount.size());
		System.out.println("Number of DataTypes: " + getTotalDType(mSubjCount));
		System.out.println(
				"Number of selected Datasets(endpoints with dataTypes): " + getSelectedEndPoints(mSubjEndPoint));
		System.out.println("Number of endPoints (Still some error): " + mEndPointError.size());}
		start = System.currentTimeMillis();
		System.out.println("Insert Subjects and counts into a relational DB.");
		addDB(mSubjCount, mSubjEndPoint);
		if(DEBUG) generateFile(mEndPointError, "EndPointErrors.csv");
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time(Add DB): " + totalTime);

	}

	private static int getSelectedEndPoints(Map<String, String> mSubjEndPoint) {
		Set<String> setTest = new HashSet<String>();
		setTest.addAll(mSubjEndPoint.values());
		return setTest.size();
	}

	private static Integer getTotalDType(Map<String, Integer> mSubjCount) {
		// mSubjCount.entrySet().parallelStream().forEach(action);
		Integer iSum = 0;
		for (Integer elem : mSubjCount.values()) {
			iSum += elem;
		}
		return iSum;
	}

	private static Set<String> getTriples(String endPoint) {
		Set<String> setReturn = new HashSet<String>();
		String sparqlQueryString = "SELECT * WHERE { ?s ?p ?o }";

		Query query = QueryFactory.create(sparqlQueryString);

		QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query);

		try {
			ResultSet results = qexec.execSelect();
			for (; results.hasNext();) {
				try {
					QuerySolution soln = results.nextSolution();
					if(!soln.get("?o").isLiteral()) continue;
					String subj = soln.get("?s").toString();
					String pred = soln.get("?p").toString();
					String obj = soln.get("?o").toString();
					
					String line = subj + "\t" + pred + "\t" + obj;
					setReturn.add(line);
				} catch (Exception e) {
				}
				// result.add(soln.get("?strLabel").toString());
				// System.out.println(soln.get("?strLabel"));
			}
		} catch (Exception e) {
			mEndPointError.put(endPoint, e.getMessage());
			return setReturn;
		}

		finally {
			qexec.close();
		}

		return setReturn;
	}

	/*
	 * @param debug: Will load a file with only few good endPoints, just to
	 * debug.
	 */
	private static Set<String> getEndPoints(boolean debug) throws IOException {
		//if (debug)
		//	return Files.lines(Paths.get("GoodEndPoints_1.txt")).collect(Collectors.toSet());
		//else {
			return QueryEndPoints.getGoodEndPoints();
		//}
	}

	private static void addDB(Map<String, Integer> mSubjCount, Map<String, String> mSubjEndPoint) {
		DBUtil.setAutoCommit(false);
		//mSubjCount.entrySet().parallelStream().forEach(elem -> {
		mSubjCount.entrySet().forEach(elem -> {
			String uriSubj = elem.getKey();
			int count = elem.getValue();
			String dataset = mSubjEndPoint.get(uriSubj); // Dataset = endpoint
			if (dataset != null) {
				try {
					DBUtil.insert(dataset, uriSubj, count);
				} catch (ClassNotFoundException | SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
//		mSubjCount.forEach((uriSubj, count) -> {
//			// String dataset = getDataset(uriSubj);
//			String dataset = mSubjEndPoint.get(uriSubj); // Dataset = endpoint
//															// that belongs the
//															// subject.
//			if (dataset != null) {
//				// Set<String> setTest = new HashSet<String>();
//				// setTest.addAll(mSubjEndPoint.values());
//				// System.out.println(setTest);
//				DBUtil.insert(dataset, uriSubj, count);
//			}
//		});
		System.out.println("Going to autoCommit=true...");
		DBUtil.setAutoCommit(true);
	}

	private static String getDataset(String uriSubj) {
		try {
			return uriSubj.split("/")[2];
		} catch (Exception e) {
			return uriSubj;
		}
	}

	public static boolean isDataType(String uri) {

		OntDocumentManager mgr = new OntDocumentManager();
		mgr.setProcessImports(false);
		OntModelSpec s = new OntModelSpec(OntModelSpec.OWL_MEM);
		s.setDocumentManager(mgr);
		OntModel ontModel = ModelFactory.createOntologyModel(s);
		// from
		// http://www.programcreek.com/java-api-examples/index.php?source_dir=szeke-master/src/main/java/edu/isi/karma/modeling/ontology/OntologyHandler.java

		DatatypeProperty dp = ontModel.getDatatypeProperty(uri);
		
		if (dp != null)
			return true;

		return false;
	}

	/*
	 * if is not an URI then is a DataType.
	 */
	public static boolean isDataTypeLiteral(String uri) {
		return !uri.startsWith("http");
	}

	public static File generateFile(Map<String, String> endPointErrors, String fileName) {
		File ret = new File(fileName);
		try {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			endPointErrors.forEach((endPoint, error) -> {
				writer.println(endPoint + "\t" + error);
			});
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret;
	}
}
