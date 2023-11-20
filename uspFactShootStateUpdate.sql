USE [Onlinestore]
GO

/****** Object:  StoredProcedure [Report].[uspFactShootStateUpdate]    Script Date: 20/11/2023 21:39:46 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create procedure [Report].[uspFactShootStateUpdate] @ShootStateEventFeed [ETL].[udtFactShootState] READONLY
AS
BEGIN

--declare @StudiosEventFeed [ETL].[UdtFactShootState]
--INSERT INTO @StudiosEventFeed 
--values
--(
--			 2--correlationId
--			,102--ShootStateId
--			,58--EventTypeId
--			,2--ShootTimeId
--			,20231015--ShootDateId
--			,2--StudioId
--			,3--ShootTypeId
--			,1--Isshootstarted
--			,'2023-10-15 09:30:00.00'--ShootStartedDateTime
--			,201--IssueId
--			,'Other'--Reason
--			,'Other1'--Comments
--			,1 --UserId
--			,1 --IsissueResolved
--			,'2023-10-15 10:00:00.00'--ShootPausedDatetime
--			,'2023-10-15 10:15:00.00'--ShootResumeDateTime
--			,'2023-10-15 09:30:00.00'--ExpectedMorningstartTime
--			,'2023-10-15 13:45:00.00'--ExpectedAfternoonStarttime
--			,'2023-10-15 18:00:00.00'--ExpectedEveningStarttime
--			,'2023-10-15 10:15:00.00'--EventTimeStamp
--			--,getdate()--lastupdatedDatetime
--			)

	--merge statement to update or insert data
	merge into report.factshootstateTBD as target
	using 
	(

		select
		s.correlationId,
		s.ShootStateId,
		s.EventTypeId,
		s.ShootTimeId,
		s.ShootDateId,
		s.StudioId,
		s.ShootTypeId,
		s.Isshootstarted,
		s.ShootStartedDateTime,
		s.IssueId,
		s.Reason,
		s.Comments,
		s.UserId,
		s.IsissueResolved,
		s.ShootPauseDateTime,
		s.ShootResumeDateTime,
		s.ExpectedMorningstartTime,
		s.ExpectedAfternoonStarttime,
		s.ExpectedEveningStarttime,
		s.EventTimeStamp
		--s.LastUpdatedTimeStamp
		from @ShootStateEventFeed s
		) as source 
		on target.studioid=source.studioId
		and
		target.shootdateId=source.shootDateid 
		and 
		target.IssueId=source.IssueId

		when matched then
		update set
		target.shootresumedatetime=source.shootresumeDatetime,
		target.eventtypeid=source.eventtypeid

		when not matched by target then 
		insert
		(correlationId,
		 ShootStateId,
		 EventTypeId,
		 ShootTimeId,
		 ShootDateId,
		 StudioId,
		 ShootTypeId,
		 Isshootstarted,
		 ShootStartedDateTime,
		 IssueId,
		 Reason,
		 Comments,
		 UserId,
		 IsissueResolved,
		 ShootPauseDateTime,
		 ShootResumeDateTime,
		 ExpectedMorningstartTime,
		 ExpectedAfternoonStarttime,
		 ExpectedEveningStarttime,
		 EventTimeStamp
		-- LastUpdatedTimeStamp
		 )
		 values
		 (
		  source.correlationId,
		  source.ShootStateId,
		  source.EventTypeId,
		  source.ShootTimeId,
		  source.ShootDateId,
		  source.StudioId,
		  source.ShootTypeId,
		  source.Isshootstarted,
		  source.ShootStartedDateTime,
		  source.IssueId,
		  source.Reason,
		  source.Comments,
		  source.UserId,
		  source.IsissueResolved,
		  source.ShootPauseDateTime,
		  source.ShootResumeDateTime,
		  source.ExpectedMorningstartTime,
		  source.ExpectedAfternoonStarttime,
		  source.ExpectedEveningStarttime,
		  source.EventTimeStamp
		 -- source.LastUpdatedTimeStamp
);
end
GO


