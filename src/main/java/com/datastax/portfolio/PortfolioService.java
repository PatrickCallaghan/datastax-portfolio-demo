package com.datastax.portfolio;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.portfolio.dao.PortfolioDao;
import com.datastax.portfolio.model.Trade;

public class PortfolioService {

	private Logger logger = LoggerFactory.getLogger(PortfolioService.class); 
		
	private PortfolioDao dao;

	private ConcurrentLinkedQueue<Trade> streamingQueue;
	private ConcurrentLinkedQueue<Portfolio> writerQueue;
	
	public static final int CHUNK=10;

	public PortfolioService(PortfolioDao dao, int noOfThreads, 
			ConcurrentLinkedQueue<Trade> streamingQueue){
		
		this.dao = dao;		
		this.streamingQueue = streamingQueue;		
	}

	public void readTradesForPortfolio(int portfolioId) {
		
		for (int chunkId = 0; chunkId < CHUNK; chunkId++){			
			dao.populateTrades(new Portfolio(portfolioId, null), chunkId, streamingQueue); 					
		}	
	}
	
	public void writeTradesForPortfolio(final Portfolio portfolio) {
		
		
		final int chunkSize = portfolio.getTrades().size() / PortfolioService.CHUNK;
		final CountDownLatch latch = new CountDownLatch(PortfolioService.CHUNK);
		
		for (int chunkId = 0; chunkId < PortfolioService.CHUNK; chunkId++) {

			final int chunkIdFinal = chunkId;
			
			logger.info("Inserting chunk " + chunkId + " of " + PortfolioService.CHUNK + " for portfolio id : " + portfolio.getPortfolioId());
			
			new Thread(new Runnable(){
				
				@Override
				public void run() {
					List<Trade> trades = portfolio.getTrades().subList(chunkIdFinal * chunkSize, (chunkIdFinal+1) * chunkSize);
					dao.insertTrades(new Portfolio(portfolio.getPortfolioId(), trades), chunkIdFinal, latch);
				}														
			}).start();
		}		
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
