package com.lf.xcode.rds.demo.base;

public class RedisClientConfiguration {
	private String hostName;
	private int port = 6379;
	private int database = 6;
	private String password;
	private int timeout = 3000;
	private int maxTotal = 500;
	private int maxIdle = 100;
	private int minIdle = 5;
	private int maxWaitMillis = 15000;
	private int minEvictableIdleTimeMillis = 300000;
	
	public String getHostName() {
		return this.hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public int getPort() {
		return this.port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getDatabase() {
		return this.database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public String getPassword() {
		return this.password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getTimeout() {
		return this.timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public int getMaxTotal() {
		return this.maxTotal;
	}

	public void setMaxTotal(int maxTotal) {
		this.maxTotal = maxTotal;
	}

	public int getMaxIdle() {
		return this.maxIdle;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public int getMinIdle() {
		return this.minIdle;
	}

	public void setMinIdle(int minIdle) {
		this.minIdle = minIdle;
	}

	public int getMaxWaitMillis() {
		return this.maxWaitMillis;
	}

	public void setMaxWaitMillis(int maxWaitMillis) {
		this.maxWaitMillis = maxWaitMillis;
	}

	public int getMinEvictableIdleTimeMillis() {
		return this.minEvictableIdleTimeMillis;
	}

	public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
		this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
	}
}
