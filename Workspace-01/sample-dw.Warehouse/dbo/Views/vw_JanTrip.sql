-- Auto Generated (Do not modify) 494DC65F7C03278156CCBC3B8A90546AACC6509DC90229A11A2EB3A18B7C79B7
CREATE VIEW [dbo].[vw_JanTrip] AS (SELECT 
     D.DayName, 
     AVG(T.TripDurationSeconds) AS AvgDuration, 
     AVG(T.TripDistanceMiles) AS AvgDistance 
 FROM dbo.Trip AS T
 JOIN dbo.[Date] AS D
     ON T.[DateID]=D.[DateID]
WHERE D.Month = 1     
 GROUP BY D.DayName)