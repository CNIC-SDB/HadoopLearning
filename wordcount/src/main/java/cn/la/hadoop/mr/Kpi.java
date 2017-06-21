package cn.la.hadoop.mr;

public class Kpi {
	private String remote_addr;//记录客户端的ip地址
	private String remote_user;//记录客户端的名称，忽略属性“-”
	private String time_local;//记录访问时间与时区
	private String request;//记录请求的url与http协议
	private String status;//记录请求状态
	private String body_bytes_sent;//记录发送给客户端文件主题内容大小
	private String http_referer;//用来记录从哪个页面链接访问过来的
	private String http_user_agent;//记录客户浏览器相关信息
	
	private boolean valid=true;

	public String getRemote_user() {
		return remote_user;
	}

	public void setRemote_user(String remote_user) {
		this.remote_user = remote_user;
	}

	public String getRemote_addr() {
		return remote_addr;
	}

	public void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}

	public String getTime_local() {
		return time_local;
	}

	public void setTime_local(String time_local) {
		this.time_local = time_local;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getBody_bytes_sent() {
		return body_bytes_sent;
	}

	public void setBody_bytes_sent(String body_bytes_sent) {
		this.body_bytes_sent = body_bytes_sent;
	}

	public String getHttp_referer() {
		return http_referer;
	}

	public void setHttp_referer(String http_referer) {
		this.http_referer = http_referer;
	}

	public String getHttp_user_agent() {
		return http_user_agent;
	}

	public void setHttp_user_agent(String http_user_agent) {
		this.http_user_agent = http_user_agent;
	}

	public boolean isValid() {
		return valid;
	}

	public void setValid(boolean valid) {
		this.valid = valid;
	}
	
	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append("valid:"+this.valid);
		sb.append("\nremote_addr:"+this.remote_addr);
		sb.append("\nremote_user:"+this.remote_user);
		sb.append("\ntime_local:"+this.time_local);
		sb.append("\nrequest:"+this.request);
		sb.append("\nstatus:"+this.status);
		sb.append("\nbody_bytes_sent:"+this.body_bytes_sent);
		sb.append("\nhttp_referer:"+this.http_referer);
		sb.append("\nhttp_user_agent:"+this.http_user_agent);
		return sb.toString();
	}
	
	public static void main(String[] args){
		String line="118.244.237.5 - - [18/Apr/2015:01:01:17 +0800] \"GET /home.php?mod=spacecp&ac=follow&op=checkfeed&rand=1429290077 HTTP/1.0\" 200 946 \"http://www.aboutyun.com/home.php?mod=spacecp\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.11 Safari/537.36\"";
		for(String element:line.split(" ")){
			System.out.println(element);
		}
		System.out.println(parser(line));
	}
	
	public static Kpi parser(String line){
		System.out.println(line);
		Kpi kpi=new Kpi();
		String[] arr=line.split(" ");
		if(arr.length>11){
			try {
				kpi.setRemote_addr(arr[0]);
				kpi.setRemote_user(arr[1]);
				kpi.setTime_local(arr[3].substring(1));
				kpi.setRequest(arr[6]);
				kpi.setStatus(arr[8]);
				kpi.setBody_bytes_sent(arr[9]);
				kpi.setHttp_referer(arr[10]);
				if(arr.length>12){
					kpi.setHttp_user_agent(arr[11]+" "+arr[12]);
				}else{
					kpi.setHttp_user_agent(arr[11]);
				}
				if(!"GET".equalsIgnoreCase(arr[5].replace("\"", "").trim())&&!"POST".equalsIgnoreCase(arr[5].replace("\"", "").trim()))
					kpi.setValid(false);
				else if(Integer.parseInt(kpi.getStatus())>=400)
					kpi.setValid(false);
			} catch (Exception e) {
				kpi.setValid(false);
			}
		}else{
			kpi.setValid(false);
		}
		return kpi;
	}
}
