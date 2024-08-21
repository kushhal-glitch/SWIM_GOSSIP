import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsTracker {
    private AtomicLong totalPingLatency = new AtomicLong(0);
    private AtomicLong totalGossipLatency = new AtomicLong(0);
    private AtomicLong totalLeaderElectionTime = new AtomicLong(0);
    private AtomicInteger pingCount = new AtomicInteger(0);
    private AtomicInteger gossipCount = new AtomicInteger(0);
    private AtomicInteger messagesSent = new AtomicInteger(0);
    private AtomicInteger messagesReceived = new AtomicInteger(0);

    public void recordPingLatency(long latency) {
        totalPingLatency.addAndGet(latency);
        pingCount.incrementAndGet();
    }

    public void recordGossipLatency(long latency) {
        totalGossipLatency.addAndGet(latency);
        gossipCount.incrementAndGet();
    }

    public void recordLeaderElectionTime(long time) {
        totalLeaderElectionTime.addAndGet(time);
    }

    public void incrementMessagesSent() {
        messagesSent.incrementAndGet();
    }

    public void incrementMessagesReceived() {
        messagesReceived.incrementAndGet();
    }

    public double getAveragePingLatency() {
        System.out.println((double) pingCount.get());
        return pingCount.get() == 0 ? 0 : totalPingLatency.get() / (double) pingCount.get();

    }

    public double getAverageGossipLatency() {
        System.out.println((double) gossipCount.get());
        return gossipCount.get() == 0 ? 0 : totalGossipLatency.get() / (double) gossipCount.get();
    }

    public long getTotalLeaderElectionTime() {
        return totalLeaderElectionTime.get();
    }

    public int getMessagesSent() {
        return messagesSent.get();
    }

    public int getMessagesReceived() {
        return messagesReceived.get();
    }

    public void printMetrics() {
        System.out.println("Average Ping Latency: " + getAveragePingLatency() + " ms");
        System.out.println("Average Gossip Latency: " + getAverageGossipLatency() + " ms");
        System.out.println("Total Leader Election Time: " + getTotalLeaderElectionTime() + " ms");
        System.out.println("Total Messages Sent: " + getMessagesSent());
        System.out.println("Total Messages Received: " + getMessagesReceived());
    }
}
