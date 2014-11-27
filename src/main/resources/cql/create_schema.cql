create keyspace if not exists datastax_portfolio_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use datastax_portfolio_demo; 
 
create table if not exists portfolio (
	portfolio_id int,
	chunk_id int,
	trade_id int,
	text1 text,
	text2 text,
	text3 text,
	text4 text,
	text5 text,
	text6 text,
	text7 text,
	text8 text,
	text9 text,
	text10 text,
	asofdate timestamp,
	PRIMARY KEY ((portfolio_id, chunk_id), trade_id)
);

truncate portfolio;



