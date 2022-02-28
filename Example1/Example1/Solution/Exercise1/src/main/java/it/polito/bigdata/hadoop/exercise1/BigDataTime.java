package it.polito.bigdata.hadoop.exercise1;

public class BigDataTime {

	static public int computeDuration(String startTime, String endTime) {
		
		//20160606_18:00,20160606_18:34
		String[] fields=startTime.split("_");
		
		int dates=Integer.parseInt(fields[0]);
		
		String[] times=fields[1].split(":");
		
		int hours=Integer.parseInt(times[0]);
		int mins=Integer.parseInt(times[1]);

		String[] fields2=endTime.split("_");
		
		int datee=Integer.parseInt(fields2[0]);
		
		String[] timee=fields2[1].split(":");
		
		int houre=Integer.parseInt(timee[0]);
		int mine=Integer.parseInt(timee[1]);
		
		
		int diffMins=(datee-dates)*24*60+(houre-hours)*60+(mine-mins);
		
		return diffMins;
	
	}
}
