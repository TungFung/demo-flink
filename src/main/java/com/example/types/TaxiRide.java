package com.example.types;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Locale;

/**
 * TaxiRide 表示出租车启动或者停止的时候发送的事件
 */
public class TaxiRide implements Serializable {

	private static transient DateTimeFormatter timeFormatter =
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

	// 每一次乘车的唯一标识
	private long rideId;
	// 每一辆出租车的唯一标识
	private long taxiId;
	// 每一个出租车司机的唯一标识
	private long driverId;
	// 标识是启动事件还是停止事件，如果是 TRUE 的话则表示是启动事件，否则表示停止事件
	private boolean isStart;
	//	每次乘车时出租车启动时间
	private DateTime startTime;
	//	每次乘车时出租车结束时间
	// 如果是启动事件的话，那么结束时间的值就是 "1970-01-01 00:00:00"
	private DateTime endTime;
	// 出租车启动的时候所在位置的经度
	private float startLon;
	// 出租车启动的时候所在位置的纬度
	private float startLat;
	// 出租车停止的时候所在位置的经度
	private float endLon;
	// 出租车停止的时候所在位置的纬度
	private float endLat;
	// 出租车乘客的数量
	private short passengerCnt;


	public TaxiRide() {
		this.startTime = new DateTime();
		this.endTime = new DateTime();
	}

	public static TaxiRide fromString(String line) {

		String[] tokens = line.split(",");
		if (tokens.length != 11) {
			throw new RuntimeException("Invalid record: " + line);
		}

		TaxiRide ride = new TaxiRide();

		try {
			ride.rideId = Long.parseLong(tokens[0]);

			switch (tokens[1]) {
				case "START":
					ride.isStart = true;
					// 第 3 个字段表示起始时间
					ride.startTime = DateTime.parse(tokens[2], timeFormatter);
					// 第 4 个字段表示结束时间
					ride.endTime = DateTime.parse(tokens[3], timeFormatter);
					break;
				case "END":
					ride.isStart = false;
					// 第 3 个字段表示结束时间
					ride.endTime = DateTime.parse(tokens[2], timeFormatter);
					// 第 4 个字段表示起始时间
					ride.startTime = DateTime.parse(tokens[3], timeFormatter);
					break;
				default:
					throw new RuntimeException("Invalid record: " + line);
			}

			ride.startLon = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
			ride.startLat = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
			ride.endLon = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
			ride.endLat = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
			ride.passengerCnt = Short.parseShort(tokens[8]);
			ride.taxiId = Long.parseLong(tokens[9]);
			ride.driverId = Long.parseLong(tokens[10]);

		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}

		return ride;
	}

	public void copyData(TaxiRide ride) {
		this.rideId = ride.getRideId();
		this.isStart = ride.isStart();
		this.startTime = ride.getStartTime();
		this.endTime = ride.getEndTime();
		this.startLon = ride.getStartLon();
		this.startLon = ride.getStartLon();
		this.endLon = ride.getEndLon();
		this.endLat = ride.getEndLat();
		this.passengerCnt = ride.getPassengerCnt();
		this.taxiId = ride.getTaxiId();
		this.driverId = ride.getDriverId();
	}

	public long getRideId() {
		return rideId;
	}

	public boolean isStart() {
		return isStart;
	}

	public DateTime getStartTime() {
		return startTime;
	}

	public DateTime getEndTime() {
		return endTime;
	}

	public float getStartLon() {
		return startLon;
	}

	public float getStartLat() {
		return startLat;
	}

	public float getEndLon() {
		return endLon;
	}

	public float getEndLat() {
		return endLat;
	}

	public short getPassengerCnt() {
		return passengerCnt;
	}

	public long getTaxiId() {
		return taxiId;
	}

	public long getDriverId() {
		return driverId;
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof TaxiRide &&
				this.rideId == ((TaxiRide) other).rideId;
	}

	@Override
	public int hashCode() {
		return (int)this.rideId;
	}


	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(rideId).append(",");
		sb.append(isStart ? "START" : "END").append(",");
		sb.append(startTime.toString(timeFormatter)).append(",");
		sb.append(endTime.toString(timeFormatter)).append(",");
		sb.append(startLon).append(",");
		sb.append(startLat).append(",");
		sb.append(endLon).append(",");
		sb.append(endLat).append(",");
		sb.append(passengerCnt).append(",");
		sb.append(taxiId).append(",");
		sb.append(driverId);

		return sb.toString();
	}

}
