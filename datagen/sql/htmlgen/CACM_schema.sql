
-- DROPS:
drop table rankings cascade;
drop table uservisits cascade;

CREATE TABLE Rankings(
	pageRank INT,
	pageURL VARCHAR(100),
	avgDuration INT
);

CREATE TABLE UserVisits(
	sourceIPAddr VARCHAR(16),
	destinationURL VARCHAR(100),
	visitDate DATE,
	adRevenue FLOAT,
	UserAgent VARCHAR(64),
	cCode CHAR(3),
	lCode CHAR(6),
	sKeyword VARCHAR(32),
	avgTimeOnSite INT
);

ALTER TABLE Rankings
 add primary key (pageURL);
