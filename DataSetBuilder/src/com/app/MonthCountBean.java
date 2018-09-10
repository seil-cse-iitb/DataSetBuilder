package com.app;

public class MonthCountBean {

	private String month;
	private String count;
	
	MonthCountBean(String month, String count){
		this.month=month;
		this.count=count;
	}
	
	public void setMonth(String month) {
		this.month=month;
	}
	
	public String getMonth() {
		return month;
	}
	
	public void setCount(String count) {
		this.count=count;
	}
	
	public String getCount() {
		return count;
	}
	@Override
    public String toString() {
        return "MonthCount{" +
                "month='" + month + '\'' +
                ", count='" + count + '\'' +
                '}';
    }
}
