package servlets;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class Find
 */
public class Find extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Find() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		handleRequestAndRespond(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		handleRequestAndRespond(request, response);
	}

	private void handleRequestAndRespond(HttpServletRequest request, HttpServletResponse response) {
		try {
			if(request.getParameter("uri") != null) {
				String uri = request.getParameter("uri");
				uri = uri.replaceAll("123nada", "#").trim(); // to solve some problems with QueryString.
				request.getSession().setAttribute("uri", uri);
				
				Map<String, Integer> result = DBUtil.findEndPoint(uri);

				if((result != null) && (result.size() > 0))
				{
					response.getOutputStream().println("<table border='1'> "
							+ "<tr> "
							+ "<th>EndPoint</th> "
							+ "<th>Count DataType</th> "
							+ "</tr>");
					result.entrySet().forEach(elem ->{
						String endPoint = elem.getKey();
						int dType = elem.getValue();
						try {
							response.getOutputStream().println("<tr> "
									+ "<td>"+ endPoint +"</td> "
									+ "<td>"+ dType +"</td> "
									+ "</tr>");
						} catch (IOException e) {
							e.printStackTrace();
						}
					});
					response.getOutputStream().println("</table>");
				}else{
					response.getOutputStream().println("<h1>NOTHING !</h1>");
				}
				//response.getWriter().write(sameas.getSameAsURI(request.getParameter("uri"),true));
			}else if(request.getParameter("endpoint") != null) {	
				findURIS(request, response);
			}else if(request.getParameter("SQL") != null) {	
				findSQL(request, response);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void findSQL(HttpServletRequest request, HttpServletResponse response) throws IOException {
		response.getOutputStream().println("<h1>Not implemented yet</h1>");
		
	}

	private void findURIS(HttpServletRequest request, HttpServletResponse response) throws IOException {
		//response.getOutputStream().println("<h1>Not implemented yet</h1>");
		String endpoint = request.getParameter("endpoint");
		endpoint = endpoint.replaceAll("123nada", "#").trim(); // to solve some problems with QueryString.
		request.getSession().setAttribute("endpoint", endpoint);
		
		Map<String, Integer> result = DBUtil.findAllURIDataTypes(endpoint);

		if((result != null) && (result.size() > 0))
		{
			String json = "[";
			for (Map.Entry<String, Integer> elem : result.entrySet()) {
				String uri = elem.getKey();
				int dType = elem.getValue();
				json += ",{ \"uri\":\""+ uri + "\",\"CountDataType\":\""+ dType + "\"}";
			}
			json = json.replaceFirst(",", "");
			json += "]";
			response.getOutputStream().println(json);
//			response.getOutputStream().println("<table border='1'> "
//					+ "<tr> "
//					+ "<th>URI</th> "
//					+ "<th>Count DataType</th> "
//					+ "</tr>");
//			result.entrySet().forEach(elem ->{
//				String uri = elem.getKey();
//				int dType = elem.getValue();
//				try {
//					response.getOutputStream().println("<tr> "
//							+ "<td>"+ uri +"</td> "
//							+ "<td>"+ dType +"</td> "
//							+ "</tr>");
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			});
//			response.getOutputStream().println("</table>");
		}else{
			response.getOutputStream().println("<h1>NOTHING !</h1>");
		}
	}

}
