package com.example;

public class Event {
    public String event_id;
    public String user_id;
    public Long event_time;

    public Event() {}

    public Event(String event_id, String user_id) {
        this.event_id = event_id;
        this.user_id = user_id;
        // this.event_time = event_time;
    }

    @Override
    public String toString() {
        return "Event{" +
                "event_id='" + event_id + '\'' +
                ", user_id='" + user_id + '\'' +
                // ", event_time=" + event_time +
                '}';
    }
}
