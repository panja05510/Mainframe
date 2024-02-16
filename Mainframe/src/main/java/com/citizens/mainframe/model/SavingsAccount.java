package com.citizens.mainframe.model;

public class SavingsAccount {
private long accountNumber;
private int routingNumber;
public long getAccountNumber() {
	return accountNumber;
}
public void setAccountNumber(long accountNumber) {
	this.accountNumber = accountNumber;
}
public int getRoutingNumber() {
	return routingNumber;
}
public SavingsAccount(long accountNumber, int routingNumber) {
	super();
	this.accountNumber = accountNumber;
	this.routingNumber = routingNumber;
}
public SavingsAccount() {
	super();
}
public void setRoutingNumber(int routingNumber) {
	this.routingNumber = routingNumber;
}
 public String toString() {
	 return "{"+"account number:"+this.accountNumber+","+"routing number:"+this.routingNumber+"}";
	 
 }


}
