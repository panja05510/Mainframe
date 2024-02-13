package com.citizens.mainframe.model;

public class AccountDetails {
	private String name;
	private String ifsc;
	private int balance;
	private String accountType;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getIfsc() {
		return ifsc;
	}
	public void setIfsc(String ifsc) {
		this.ifsc = ifsc;
	}
	public int getBalance() {
		return balance;
	}
	public void setBalance(int balance) {
		this.balance = balance;
	}
	public String getAccountType() {
		return accountType;
	}
	public void setAccountType(String accountType) {
		this.accountType = accountType;
	}
	
		@Override
	public String toString() {
		return "{"+"\"name\":" +"\""+ name +"\""+ ", \"ifsc\":" + "\""+ifsc +"\""+ ", \"balance\":" + balance + ", \"accountType\":"
				+"\""+ accountType+"\""+"}" ;
	}
		public AccountDetails() {
		super();
		// TODO Auto-generated constructor stub
		
	}
	public AccountDetails(String name, String ifsc, int balance, String accountType) {
		super();
		this.name = name;
		this.ifsc = ifsc;
		this.balance = balance;
		this.accountType = accountType;
	}
	
	

}
