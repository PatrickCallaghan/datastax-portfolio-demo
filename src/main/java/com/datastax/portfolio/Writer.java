package com.datastax.portfolio;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.portfolio.dao.PortfolioDao;
import com.datastax.portfolio.model.Trade;

public class Writer {

	private static Logger logger = LoggerFactory.getLogger(Writer.class);
	private static int BATCH = 10000;
	private static AtomicLong tradeId;
	private boolean writer = false;

	// Create shared queue
	private BlockingQueue<Portfolio> writerQueue = new ArrayBlockingQueue<Portfolio>(5);

	public Writer() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "127.0.0.1");
		String noOfPortfoliosStr = PropertyHelper.getProperty("noOfPortfolios", "1");
		String noOfTradesStr = PropertyHelper.getProperty("noOfTrades", "100000");

		int noOfThreads = PortfolioService.CHUNK;
		int noOfPortfolios = Integer.parseInt(noOfPortfoliosStr);
		int noOfTrades = Integer.parseInt(noOfTradesStr);

		// Start
		PortfolioDao dao = new PortfolioDao(contactPointsStr.split(","));
		PortfolioService portfolioService = new PortfolioService(dao, noOfThreads, null);

		Timer timer = new Timer();
		logger.info("Writing " + noOfTradesStr + " trades for " + noOfPortfolios + " Porfolios.");
		timer.start();
		
		for (int portfolioId = 0; portfolioId < noOfPortfolios; portfolioId++) {

			portfolioService.writeTradesForPortfolio(createTrades(portfolioId, noOfTrades));
		}
		// Ensure all have finished		
		timer.end();
		dao.close();		
		logger.info("Portfolio load took " + timer.getTimeTakenSeconds() + " secs.");		
	}

	private void sleep(long seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private Portfolio createTrades(int portfolioId, int noOfTrades) {

		List<Trade> trades = new ArrayList<Trade>(noOfTrades);
		AtomicLong tradeId = new AtomicLong(1);

		for (int i = 0; i < noOfTrades; i++) {

			String id = portfolioId + "-" + (i+1);

			Trade trade = new Trade();
			trade.setTradeId(tradeId.incrementAndGet());
			trade.setText1("text1-" + id);
			trade.setText2("text2-" + id);
			trade.setText3("text3-" + id);
			trade.setText4("text4-" + id);
			trade.setText5("text5-" + id);
			trade.setText6("text6-" + id);
			trade.setText7("text7-" + id);
			trade.setText8("text8-" + id);
			trade.setText9("text9-" + id);
			trade.setText10("text10-" + id);
			trade.setText11("text11-" + id);
			trade.setText12("text12-" + id);
			trade.setText13("text13-" + id);
			trade.setText14("text14-" + id);
			trade.setText15("text15-" + id);
			trade.setText16("text16-" + id);
			trade.setText17("text17-" + id);
			trade.setText18("text18-" + id);
			trade.setText19("text19-" + id);
			trade.setText20("text20-" + id);
			trade.setAsOfDate(new Date());

			trades.add(trade);
		}

		return new Portfolio(portfolioId, trades);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Writer();

		System.exit(0);
	}
}
