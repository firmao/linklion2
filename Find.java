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
			if(request.getParameter("uri") != null)
			{
				String uri = request.getParameter("uri");
				uri = uri.replaceAll("123nada", "#").trim(); // to solve some problems with QueryString.
				request.getSession().setAttribute("uri", uri);
				
				Map<String, Integer> result = DBUtil.findEndPoint(uri);

				if((result != null) && (result.size() > 0))
				{
					//response.getOutputStream().println(result.toString());
					result.entrySet().forEach(elem ->{
						String endPoint = elem.getKey();
						int dType = elem.getValue();
						try {
							response.getOutputStream().println(endPoint + " | " + dType);
						} catch (IOException e) {
							e.printStackTrace();
						}
					});
				}else{
					response.getOutputStream().println("<h1>NOTHING !<h1>");
				}
				//response.getWriter().write(sameas.getSameAsURI(request.getParameter("uri"),true));
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
