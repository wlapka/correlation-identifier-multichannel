/**
 * @author wlapka
 *
 * @created May 7, 2014 4:18:59 PM
 */
package net.thoiry.lapka.correlationidentifier;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wlapka
 * 
 */
public class Requestor implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(Requestor.class);
	private static final int TIMEOUTINSECONDS = 1;
	private static final AtomicLong NEXTID = new AtomicLong(1);
	private static final int NUMBEROFTHREADS = 2;
	private final ConcurrentMap<Long, Message> sentRequests = new ConcurrentHashMap<>();
	private final ExecutorService executorService = Executors.newFixedThreadPool(NUMBEROFTHREADS);
	private final CountDownLatch internalCountDownLatch = new CountDownLatch(NUMBEROFTHREADS);
	private final Sender sender;
	private final ReplyReceiver replyReceiver;
	private final CountDownLatch countDownLatch;

	public Requestor(BlockingQueue<Message> requestQueue, ConcurrentMap<Long, Message> replyMap,
			CountDownLatch countDownLatch) {
		this.sender = new Sender(requestQueue, this.sentRequests, this.internalCountDownLatch);
		this.replyReceiver = new ReplyReceiver(replyMap, this.sentRequests, this.internalCountDownLatch);
		this.countDownLatch = countDownLatch;
	}

	@Override
	public void run() {
		try {
			executorService.submit(sender);
			executorService.submit(replyReceiver);
		} finally {
			if (this.countDownLatch != null) {
				this.countDownLatch.countDown();
			}
		}
	}

	public void stop() {
		try {
			this.sender.stop();
			this.replyReceiver.stop();
			this.internalCountDownLatch.await();
			this.executorService.shutdown();
		} catch (InterruptedException e) {
			LOGGER.error("Exception occured while stopping.", e);
			Thread.currentThread().interrupt();
			throw new RuntimeException(e.getMessage(), e);
		}
		LOGGER.info("Received stop signal");
	}

	private static class ReplyReceiver implements Runnable {

		private volatile boolean stop = false;
		private final ConcurrentMap<Long, Message> replyMap;
		private final ConcurrentMap<Long, Message> sentRequests;
		private final CountDownLatch countDownLatch;

		public ReplyReceiver(ConcurrentMap<Long, Message> replyMap, ConcurrentMap<Long, Message> sentRequests,
				CountDownLatch countDownLatch) {
			this.replyMap = replyMap;
			this.sentRequests = sentRequests;
			this.countDownLatch = countDownLatch;
		}

		@Override
		public void run() {
			try {
				while (!this.stop) {
					this.processReplies();
				}
			} finally {
				if (this.countDownLatch != null) {
					this.countDownLatch.countDown();
				}
			}
		}

		private void processReplies() {
			for (Iterator<Long> iterator = this.sentRequests.keySet().iterator(); iterator.hasNext();) {
				Long correlationId = iterator.next();
				if (this.replyMap.containsKey(correlationId)) {
					Message reply = this.replyMap.remove(correlationId);
					Message request = this.sentRequests.get(reply.getCorrelationId());
					LOGGER.info("{}: Received reply for request: {}.", Thread.currentThread().getId(), request);
					iterator.remove();
				}
			}
		}

		public void stop() {
			this.stop = true;
			LOGGER.info("Received stop signal");
		}
	}

	private static class Sender implements Runnable {

		private volatile boolean stop = false;
		private final BlockingQueue<Message> requestQueue;
		private final Map<Long, Message> sentRequests;
		private final CountDownLatch countDownLatch;

		public Sender(BlockingQueue<Message> requestQueue, Map<Long, Message> sentRequests,
				CountDownLatch countDownLatch) {
			this.requestQueue = requestQueue;
			this.sentRequests = sentRequests;
			this.countDownLatch = countDownLatch;
		}

		@Override
		public void run() {
			try {
				while (!this.stop) {
					Long messageId = NEXTID.getAndIncrement();
					Message message = new Message(messageId, null, "Message number " + messageId + " from thread "
							+ Thread.currentThread().getId());
					if (!this.requestQueue.offer(message, TIMEOUTINSECONDS, TimeUnit.SECONDS)) {
						LOGGER.info("Timeout occured while trying to send request since queue full.");
						continue;
					}
					this.sentRequests.put(messageId, message);
					LOGGER.info("Request sent: '{}'.", message);
				}
			} catch (InterruptedException e) {
				LOGGER.info("Interrupted exception occured", e);
				Thread.currentThread().interrupt();
				throw new RuntimeException(e.getMessage(), e);
			} finally {
				if (this.countDownLatch != null) {
					this.countDownLatch.countDown();
				}
			}
		}

		public void stop() {
			this.stop = true;
			LOGGER.info("Received stop signal");
		}
	}
}
