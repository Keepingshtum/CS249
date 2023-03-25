package edu.sjsu.cs249.zoo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Storage implements Serializable {
    private boolean isLeader;

    private int cooldown;

    private String leaderName;

    private List<Long> lunchesAttended;

    public List<Long> getLunchesAttended() {
        return lunchesAttended;
    }

    public void setLunchesAttended(List<Long> lunchesAttended) {
        this.lunchesAttended = lunchesAttended;
    }

    public List<List<String>> getAttendeesForEachZxid() {
        return attendeesForEachZxid;
    }

    public void setAttendeesForEachZxid(List<List<String>> attendeesForEachZxid) {
        this.attendeesForEachZxid = attendeesForEachZxid;
    }

    private List<List<String>> attendeesForEachZxid;

    public boolean isSkip() {
        return skip;
    }

    public void setSkip(boolean skip) {
        this.skip = skip;
    }

    private boolean skip;

    public String getLeaderName() {
        return leaderName;
    }

    public void setLeaderName(String leaderName) {
        this.leaderName = leaderName;
    }

    public Map<String, String> getAttendeesMap() {
        return attendeesMap;
    }

    public void setAttendeesMap(Map<String, String> attendeesMap) {
        this.attendeesMap = attendeesMap;
    }

    public int getCooldown() {
        int temp = cooldown -1;
        setCooldown(0);
        return cooldown;
    }

    public void setCooldown(int cooldown) {
        this.cooldown = cooldown;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void setLeader(boolean leader) {
        isLeader = leader;
    }

    public Storage(boolean isLeader, int cooldown, Map<String, String> attendeesMap) {
        this.isLeader = isLeader;
        this.cooldown = cooldown;
        this.attendeesMap = attendeesMap;
    }

    Map<String,String> attendeesMap;

    @Override
    public String toString() {
        return "Storage{" +
                "isLeader=" + isLeader +
                ", cooldown=" + cooldown +
                ", attendeesMap=" + attendeesMap +
                '}';
    }
}
