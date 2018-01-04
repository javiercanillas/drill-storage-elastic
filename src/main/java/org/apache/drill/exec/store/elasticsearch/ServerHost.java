package org.apache.drill.exec.store.elasticsearch;

public class ServerHost {
	
	public ServerHost(String ip){
		this.ip = ip;
	}
	
	private String ip;
	
	
	
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	 
	
	

}
