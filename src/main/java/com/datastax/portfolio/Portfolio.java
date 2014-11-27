package com.datastax.portfolio;

import java.util.List;

import com.datastax.portfolio.model.Trade;

public class Portfolio {

	private int portfolioId;
	private List<Trade> trades;

	public Portfolio(int portfolioId, List<Trade> trades) {
		this.portfolioId = portfolioId;
		this.trades = trades;
	}

	public int getPortfolioId() {
		return portfolioId;
	}

	public List<Trade> getTrades() {
		return trades;
	}
}
