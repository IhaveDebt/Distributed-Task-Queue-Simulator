// DistributedTaskQueueSimulator.java
// Local simulator of a distributed task queue with workers, acknowledgements and retries
// Compile & run: javac DistributedTaskQueueSimulator.java && java DistributedTaskQueueSimulator

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * DistributedTaskQueueSimulator
 * - In-memory broker with FIFO queue
 * - Workers pull tasks, process, ack; tasks re-enqueued on timeout
 */
public class DistributedTaskQueueSimulator {
    static class Task {
        final String id;
        final String payload;
        Task(String id, String payload) { this.id = id; this.payload = payload; }
    }

    static class Broker {
        private final BlockingQueue<Task> queue = new LinkedBlockingQueue<>();
        private final ConcurrentMap<String, Long> inflight = new ConcurrentHashMap<>();
        private final ScheduledExecutorService reenqueue = Executors.newSingleThreadScheduledExecutor();
        private final long ackTimeoutMs;

        Broker(long ackTimeoutMs) {
            this.ackTimeoutMs = ackTimeoutMs;
            reenqueue.scheduleAtFixedRate(this::reclaimTimedOut, ackTimeoutMs/2, ackTimeoutMs/2, TimeUnit.MILLISECONDS);
        }

        void publish(Task t) { queue.offer(t); }

        Task pull(String workerId) throws InterruptedException {
            Task t = queue.poll(1, TimeUnit.SECONDS);
            if (t != null) inflight.put(t.id, System.currentTimeMillis());
            return t;
        }

        void ack(String taskId) { inflight.remove(taskId); }

        private void reclaimTimedOut() {
            long now = System.currentTimeMillis();
            List<String> toRequeue = new ArrayList<>();
            for (Map.Entry<String, Long> e : inflight.entrySet()) {
                if (now - e.getValue() > ackTimeoutMs) toRequeue.add(e.getKey());
            }
            for (String id : toRequeue) {
                // naive: re-create task payload with id
                System.out.println("[BROKER] Requeue timed-out task " + id);
                inflight.remove(id);
                publish(new Task(id, "requeued-" + id));
            }
        }

        void shutdown() { reenqueue.shutdownNow(); }
    }

    static class Worker implements Runnable {
        final Broker broker;
        final String id;
        final Random rnd = new Random();
        final double failRate;
        Worker(Broker b, String id, double failRate) { broker = b; this.id = id; this.failRate = failRate; }

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Task t = broker.pull(id);
                    if (t == null) continue;
                    System.out.println("[" + id + "] Pulled " + t.id);
                    // process
                    Thread.sleep(200 + rnd.nextInt(300));
                    if (rnd.nextDouble() < failRate) {
                        System.out.println("[" + id + "] Simulating crash for " + t.id);
                        // no ack -> will timeout
                        continue;
                    }
                    System.out.println("[" + id + "] Completed " + t.id);
                    broker.ack(t.id);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // Demo
    public static void main(String[] args) throws Exception {
        Broker broker = new Broker(2000);
        ExecutorService workers = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 4; i++) {
            workers.submit(new Worker(broker, "worker-" + i, i == 0 ? 0.2 : 0.05));
        }
        // publish tasks
        for (int i = 0; i < 50; i++) {
            broker.publish(new Task("task-" + i, "payload-" + i));
            Thread.sleep(50);
        }
        Thread.sleep(20000);
        workers.shutdownNow();
        broker.shutdown();
        System.out.println("Simulation ended.");
    }
}
