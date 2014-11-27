package com.datastax.portfolio.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.portfolio.Portfolio;
import com.datastax.portfolio.model.Trade;

public class PortfolioDao {

	private Logger logger = LoggerFactory.getLogger(PortfolioDao.class);
	private Session session;
	
	private static String keyspaceName = "datastax_portfolio_demo";
	private static String portfolioTable = keyspaceName + ".portfolio";

	private static final String INSERT_INTO_PORTFOLIO = "Insert into " + portfolioTable
			+ " (portfolio_id, chunk_id, trade_id, text1, text2, text3, text4, text5, text6, text7, text8, text9, text10, asOfDate) "
			+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?);";	
			
	private static final String GET_PORTFOLIO_BY_ID = "select * from " + portfolioTable + " "
			+ "where portfolio_id = ? and chunk_id = ?";
	
	private PreparedStatement insertPortfolioStmt;
	private PreparedStatement getPortfolioId;
	
	public PortfolioDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder()				
				.addContactPoints(contactPoints)				
				.build();
		
		this.session = cluster.connect();

		this.insertPortfolioStmt = session.prepare(INSERT_INTO_PORTFOLIO);
		this.getPortfolioId = session.prepare(GET_PORTFOLIO_BY_ID);
	}

	public void insertTrades(Portfolio portfolio, int chunkId, CountDownLatch latch){
		
		BoundStatement boundStmt = new BoundStatement(this.insertPortfolioStmt);
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();
		
		int portfolioId = portfolio.getPortfolioId();
	
		List<Trade> trades = portfolio.getTrades();
		
		logger.info("Inserting " + trades.size() + " for chunkId " + chunkId);
		
		for (Trade trade : trades) {
			int tradeId = new Long(trade.getTradeId()).intValue();
			
			boundStmt.setInt("portfolio_id", portfolioId);
			boundStmt.setInt("chunk_id", chunkId);
			boundStmt.setInt("trade_id", tradeId);
			
			boundStmt.setString("text1", trade.getText1());
			boundStmt.setString("text2", trade.getText1());
			boundStmt.setString("text3", trade.getText1());
			boundStmt.setString("text4", trade.getText1());
			boundStmt.setString("text5", trade.getText1());
			boundStmt.setString("text6", trade.getText1());
			boundStmt.setString("text7", trade.getText1());
			boundStmt.setString("text8", trade.getText1());
			boundStmt.setString("text9", trade.getText1());
			boundStmt.setString("text10", trade.getText1());
			boundStmt.setDate("asofdate", trade.getAsOfDate());	
			
			results.add(session.executeAsync(boundStmt));
		}

		//Wait till we have everything back.
		boolean wait = true;
		while (wait) {
			// start with getting out, if any results are not done, wait is
			// true.
			wait = false;
			for (ResultSetFuture result : results) {
				if (!result.isDone()) {
					wait = true;
					break;
				}
			}
		}
		
		logger.info("Inserted " + trades.size() + " trades");	
		latch.countDown();
	}

	public void populateTrades(Portfolio portfolio, int chunkId, ConcurrentLinkedQueue<Trade> streamingQueue) {
		
		BoundStatement stmt = new BoundStatement(this.getPortfolioId);		
		ResultSet results = session.execute(stmt.bind(portfolio.getPortfolioId(), chunkId));
		
		for (Row row : results.all()){			
			 streamingQueue.offer(createTradeFromRow(row));
		}		
	}

	private Trade createTradeFromRow(Row row) {
		
		Trade trade = new Trade();
		
		trade.setTradeId(new Integer(row.getInt("trade_id")).longValue());
		trade.setText1(row.getString("text1"));
		trade.setText2(row.getString("text2"));
		trade.setText3(row.getString("text3"));
		trade.setText4(row.getString("text4"));
		trade.setText5(row.getString("text5"));
		trade.setText6(row.getString("text6"));
		trade.setText7(row.getString("text7"));
		trade.setText8(row.getString("text8"));
		trade.setText9(row.getString("text9"));
		trade.setText10(row.getString("text10"));
		trade.setAsOfDate(row.getDate("asofdate"));
		
		return trade;		
	}
	
	public void close(){
		this.session.close();
	}
}
