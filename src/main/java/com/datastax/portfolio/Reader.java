package com.datastax.portfolio;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.portfolio.dao.PortfolioDao;
import com.datastax.portfolio.model.Trade;

public class Reader {

	private static Logger logger = LoggerFactory.getLogger(Reader.class);
	private static int BATCH = 10000;
	private static AtomicLong tradeId;

	// Create shared queue
	private BlockingQueue<Portfolio> writerQueue = new ArrayBlockingQueue<Portfolio>(5);

	private ConcurrentLinkedQueue<Trade> streamingQueue = new ConcurrentLinkedQueue<Trade>();

	public Reader() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "127.0.0.1");
		String portfolioIdStr = PropertyHelper.getProperty("portfolioId", "0");
		int noOfThreads = PortfolioService.CHUNK;
		int portfolioId = Integer.parseInt(portfolioIdStr);
			
		// Start
		PortfolioDao dao = new PortfolioDao(contactPointsStr.split(","));
		PortfolioService portfolioService = new PortfolioService(dao, noOfThreads, streamingQueue);

		Timer timer = new Timer();
		timer.start();

		Thread reader = new Thread(new Runnable() {
			@Override
			public void run() {
				AtomicLong counter = new AtomicLong(0);
				Trade trade = null;
				while (true) {
					trade = streamingQueue.poll();

					if (trade != null) {
						counter.incrementAndGet();

						if (counter.get() % 10000 == 0) {
							logger.info(counter.get() + " - " + trade.toString());
						}
					}
				}
			}
		});
		reader.start();
		
		// Start reading
		portfolioService.readTradesForPortfolio(portfolioId);	
		sleep(3);
	}

	private void sleep(long seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Reader();
		System.exit(0);
	}
}
