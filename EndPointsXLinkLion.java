import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class EndPointsXLinkLion {
	static Set<String> setDomainLinkLion = new HashSet<String>();
	static Set<String> setGoodEndPoints = null;
	public static void main(String args[]) throws IOException{
		File goodEndPoints = new File("GoodEndPoints.txt");
		File dirLinkLion = new File("D:\\Dropbox\\RunClosuresJar\\sameAs\\s1000");
		long start = System.currentTimeMillis();
		Set<String> setMatch = checkPercentage(goodEndPoints,dirLinkLion);
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time: " + totalTime);
		System.out.println("TotalDomainsLinkLion (owl:sameAs): " + setDomainLinkLion.size());
		System.out.println("TotalGoodDomainsEndPoint: " + setGoodEndPoints.size());
		System.out.println("TotalDomainsMatch: " + setMatch.size());
		double percent = 0.0d;
		System.out.println("Percentage: " + percent + "% of LinkLion domains has good EndPoints alive.");
		System.out.println(setMatch);
	}

	private static Set<String> checkPercentage(File goodEndPoints, File dirLinkLion) throws IOException {
		setGoodEndPoints = onlyDomain(Files.lines(Paths.get(goodEndPoints.getName())).collect(Collectors.toSet()));
		Set<File> setFiles = getFiles(dirLinkLion);
		
		
		setFiles.forEach(elem -> {
		//setFiles.parallelStream().forEach(elem -> {
			//if setGoodEndPoints.contais(subDomain)
			try {
				String[] sDomainSubObj = getDataSetDomain(elem);
				setDomainLinkLion.add(sDomainSubObj[0]);
				setDomainLinkLion.add(sDomainSubObj[1]);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		return compareDomains(setGoodEndPoints, setDomainLinkLion);
		
	}
	
	private static Set<String> onlyDomain(Set<String> collect) {
		Set<String> domains = new HashSet<String>();
		collect.forEach(elem -> {
		//collect.parallelStream().forEach(elem -> {
			String domain = elem.split("/")[2];
			domains.add(domain);
		});
		return domains;
	}

	private static Set<String> compareDomains(Set<String> setGoodDomains, Set<String> setDomainLinkLion) {
		Set<String> setMatch = new HashSet<String>();
		setDomainLinkLion.forEach(elem ->{
		//setDomainLinkLion.parallelStream().forEach(elem ->{
			if(setGoodDomains.contains(elem))
				setMatch.add(elem);
		});
		return setMatch;
	}

	private static Set<File> getFiles(File dir) throws IOException {
		Set<File> setFiles = null;
		if (dir.isDirectory()) {
			setFiles = Files.walk(Paths.get(dir.getPath())).filter(Files::isRegularFile).map(Path::toFile)
					.collect(Collectors.toSet());
		}
		return setFiles;
	}
	
	private static String[] getDataSetDomain(File f) throws IOException {
		String uriS = "ErrorSubject";
		String uriO = "ErrorObject";
		BufferedReader brTest = new BufferedReader(new FileReader(f));
		try {
			String line = brTest.readLine();
			uriS = line.substring(1, line.indexOf('>'));
			uriO = line.substring(line.lastIndexOf('<') + 1, line.lastIndexOf('>'));
			uriS = uriS.split("/")[2];
			uriO = uriO.split("/")[2];
			brTest.close();
		} catch (Exception e) {
			if (brTest != null)
				brTest.close();
		}
		String[] ret = { uriS, uriO };
		return ret;
	}
}
