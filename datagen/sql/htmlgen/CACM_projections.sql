
CREATE PROJECTION RankingsProj
(
pageRank ENCODING RLE,
pageURL,
avgDuration
)
as
SELECT pageRank, pageURL, avgDuration
from Rankings
ORDER BY pageRank
SEGMENTED BY HASH(pageURL)
ALL NODES;


CREATE Projection UserVisitsProj
(
date ENCODING RLE,
sourceIPAddr,
destinationURL,
adRevenue,
UserAgent,
cCode,
lCode,
sKeyword,
avgTimeOnSite
)
as
SELECT visitDate, sourceIPAddr, destinationURL, adRevenue, UserAgent, cCode, lCode, sKeyword, avgTimeOnSite
FROM UserVisits
ORDER BY visitDate
SEGMENTED BY HASH(destinationURL)
ALL NODES;

