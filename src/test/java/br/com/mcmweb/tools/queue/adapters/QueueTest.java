package br.com.mcmweb.tools.queue.adapters;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class QueueTest {

	String myString;
	int myPrimitiveInt;
	Long myLong;
	List<String> someList;
	private long time;

	public QueueTest() {
		this.setMyString("I AM A STRING! Today is: " + new Date());
		this.setMyPrimitiveInt(Integer.MAX_VALUE);
		this.setMyLong(Long.MAX_VALUE);
		List<String> someList = new ArrayList<String>();
		someList.add("First Item");
		someList.add("Second Item");
		someList.add("Third Item");
		this.setSomeList(someList);
		this.setTime(System.currentTimeMillis());
	}

	public String getMyString() {
		return myString;
	}

	public void setMyString(String myString) {
		this.myString = myString;
	}

	public int getMyPrimitiveInt() {
		return myPrimitiveInt;
	}

	public void setMyPrimitiveInt(int myPrimitiveInt) {
		this.myPrimitiveInt = myPrimitiveInt;
	}

	public Long getMyLong() {
		return myLong;
	}

	public void setMyLong(Long myLong) {
		this.myLong = myLong;
	}

	public List<String> getSomeList() {
		return someList;
	}

	public void setSomeList(List<String> someList) {
		this.someList = someList;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

}