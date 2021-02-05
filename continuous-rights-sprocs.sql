SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[prcCDI_LibraryRights_tbonghi_r] 

@LibraryHash varchar(50)

AS

/*

***********************************************************************************
** Desc: 

** Notes

DROP PROCEDURE prcCDI_LibraryRights_r

EXEC dbo.prcCDI_LibraryRights_r @LibraryHash='C6UB7VS5FD'

SELECT TOP 10 * FROM dbo.tblxCDI_AppError ORDER BY CDI_AppErrorId DESC;

DECLARE @rc int
EXEC @rc=dbo.prcCDI_LibraryRights_r @LibraryHash='C6UB7VS5FD'
SELECT ReturnCode=@rc

SELECT * FROM dbo.tblxCDI_LibraryRightsLog ORDER BY CDI_LibraryRightsLogId DESC;

select * from [dbo].[tblAlmaInstitution]

Harvard 01HVD.01HVD.PPRD / S6SE5VN7VQ

SELECT TOP 100 ai.LibraryHash,ih.HoldingsImported 
FROM dbo.tblxCDI_ImportHoldings ih
INNER JOIN dbo.tblAlmaInstitution ai ON ai.LibraryId=ih.LibraryId
WHERE State='Completed' 
ORDER BY ih.CDI_ImportHoldingsId DESC


**
** Author:	Eddie Kramer
** Date:    2019-08-13
************************************************************************************
** Change History
************************************************************************************
** Date			Author		Description
** ----------	---------	----------------------------------------------------------
** 2020-09-01	EddieK		Created per CDI-773
** 2020-09-14	EddieK		on error log CDI_AppErrorId to tblxCDI_LibraryRightsLog
							make sure CDI import for @LibraryHash is not running as this sprocs begins - else error out
************************************************************************************
*/

SET NOCOUNT ON;

DECLARE
	@LibraryId int,
	@TimeStart datetime=GETDATE(),
	@TimeEnd datetime,
	@LibraryDatabases int,
	@Monographs int,
	@Serials int,
	@DurationSec int,
	@CDI_LibraryRightsLogId int,
	@CDI_ImportHoldingsId int,
	@ErrorMessage nvarchar(1000),
	@ReturnCode int=0;

-----------------------------------------------------------------------------------------------------	
SET XACT_ABORT ON
BEGIN TRY 
-----------------------------------------------------------------------------------------------------	

-------------------------------------------------
-- Set Up
-------------------------------------------------
SELECT @LibraryId=LibraryId
FROM dbo.tblAlmaInstitution
WHERE LibraryHash=@LibraryHash;

INSERT dbo.tblxCDI_LibraryRightsLog (LibraryId) VALUES (@LibraryId);
SET @CDI_LibraryRightsLogId=SCOPE_IDENTITY();

-------------------------------------------------
-- do not proceed if CDI import is currently processing 
-------------------------------------------------
SELECT @CDI_ImportHoldingsId=CDI_ImportHoldingsId
FROM dbo.tblxCDI_ImportHoldings
WHERE
	LibraryId=@LibraryId AND
	State='Processing';

-- SET @CDI_ImportHoldingsId=1234;

IF @CDI_ImportHoldingsId IS NOT NULL BEGIN
	SET @ErrorMessage=CONCAT('Error : Concurent import for @LibraryHash=''',@LibraryHash,''' (CDI_ImportHoldingsId=',@CDI_ImportHoldingsId,')');
	RAISERROR (@ErrorMessage,16,1); 
END

-- uncomment to force/test error logging
--SELECT ForceError=1/0;

-------------------------------------------------
-- Populate #tblLibrary, #tblDatabase, 	#tblLibraryDatabase
-------------------------------------------------
	/*
	this block of code was lifted from SSDBx.dbo.prcSOA_SummonMergePrep

	SSDBx.dbo.prcSOA_SummonMergePrep populates (for all CDI institutions) :
		tblSOA_Library
		tblSOA_Database
		tblSOA_LibraryDatabase

		takes 13 seconds to run in PROD

		these tables are used by the combined 360 and CDI Summon Rights Feed

	Here we populate (for a single CDI institution) :
		#tblLibrary
		#tblDatabase
		#tblLibraryDatabase
	*/

	SELECT 
		LibraryId,
		LibraryHash,
		LibraryName,
		Institution=LibraryName,
		InstitutionCode,
        ProxyURL,
        ProxyType
	INTO dbo.#tblLibrary
	FROM dbo.tblAlmaInstitution
	WHERE LibraryId=@LibraryId;

	-- note : IsFullText logic is from SSDBx.viewDataFeed_SOA_Database
	SELECT DatabaseId, DatabaseCode, DatabaseName, ProviderId, IsFullText=CASE WHEN DatabaseId=9999999 THEN 1 ELSE 0 END,Source, ProviderCode, ProviderName, TitleURLDefault
	INTO dbo.#tblDatabase
	FROM (
		-- libific Catalog, IR, LibGuides databases
		SELECT 
			id.DatabaseId, 
			id.DatabaseCode, 
			id.DatabaseName, 
			id.ProviderId,
			Source=id.DatabaseType,
            p.ProviderCode,
            p.ProviderName,
            p.TitleURLDefault
		FROM dbo.tblAlmaInstitutionDatabase id
		INNER JOIN dbo.#tblLibrary l ON l.LibraryId=id.LibraryId
        INNER JOIN dbo.tblSF_Provider p ON p.ProviderId=id.ProviderId

		UNION  ALL

		-- shared generic database for unassigned holdings
		SELECT 
			DatabaseId, 
			DatabaseCode, 
			DatabaseName, 
			id.ProviderId,
			Source='Unassigned Holdings',
            p.ProviderCode,
            p.ProviderName,
            p.TitleURLDefault
		FROM dbo.tblSF_Database id
        INNER JOIN dbo.tblSF_Provider p ON p.ProviderId=id.ProviderId
	) x

-------------------------------------------------------------------------------------------------------------------
-- populate #tblLibraryDatabase (step 1) with Holding, Catalog, IR and LibGuides databases
-------------------------------------------------------------------------------------------------------------------
/* 
-- notes from SSDBx.dbo.prcSOA_SummonMergePrep
how tblSOA_LibraryDatabase is populated
	1) Catalog, IR and LibGuides databases :			AlmaHoldingsDB.dbo.tblAlmaInstitutionDatabase
	2) 360 and ALMA_UNASSIGNED_HOLDINGS databases :		AlmaHoldingsDB.dbo.tblSF_LibraryDatabase (populated via holdings.xml import)
	3) 360 Zero Title Databases :						AlmaHoldingsDB.dbo.tblAlmaInstitutionDatabaseZeroTitle

	it is possible for the same database to be in both tblSF_LibraryDatabase and tblAlmaInstitutionDatabaseZeroTitle
		if the database in tblSF_LibraryDatabase has actual holdings (LDTs>0) then use that record
		if the database in tblSF_LibraryDatabase has no actual holdings (LDTs=0) then use the tblAlmaInstitutionDatabaseZeroTitle record

	Step 1) 360/ALMA_UNASSIGNED_HOLDINGS databases where LDTs>0
	Step 2) Zero Title Databases (not already included)
	Step 3) 360/ALMA_UNASSIGNED_HOLDINGS databases where LDTs=0  (not already included)
	Step 4) Catalog, IR and LibGuides databases
*/
-----------------------------------------------
--Step 1) 360/ALMA_UNASSIGNED_HOLDINGS databases where LDTs>0
-----------------------------------------------
SELECT ld.LibraryDatabaseId, l.LibraryId, ld.DatabaseId, ld.CustomDBURL, ld.CustomDatabaseName, ld.LinkAuthorization, ld.IsSelect, ld.OmitProxy, l.LibraryHash, d.DatabaseCode, Source=CAST('Holdings' as varchar(20))
INTO dbo.#tblLibraryDatabase
FROM dbo.#tblLibrary l
INNER JOIN dbo.tblSF_LibraryDatabase ld ON 
	ld.LibraryId=l.LibraryId AND
	ld.LDTs>0
INNER JOIN (
	-- 360 databases from SSDBx
	SELECT DatabaseId,DatabaseCode FROM dbo.synSSDBx_tblDatabase
	UNION ALL
	-- NON-360 databases (ALMA_UNASSIGNED_HOLDINGS)
	SELECT DatabaseId,DatabaseCode FROM dbo.#tblDatabase
) d ON d.DatabaseId=ld.DatabaseId;

-----------------------------------------------
--Step 2) Zero Title Databases (not already included)
-----------------------------------------------
INSERT dbo.#tblLibraryDatabase (LibraryDatabaseId, LibraryId, DatabaseId, CustomDBURL, CustomDatabaseName, LinkAuthorization, IsSelect, OmitProxy, LibraryHash, DatabaseCode, Source) 
SELECT 
	zt.LibraryDatabaseId, 
	l.LibraryId, 
	d.DatabaseId, 
	CustomDBURL=COALESCE(zt.CustomDBURL,zt.DBURL), 
	CustomDatabaseName=COALESCE(zt.CustomDatabaseName,zt.DatabaseName), 
	zt.LinkAuthorization, 
	IsSelect=0, 
	zt.OmitProxy, 
	l.LibraryHash, 
	d.DatabaseCode, 
	Source='ZeroTitle'
FROM dbo.tblAlmaInstitutionDatabaseZeroTitle zt
INNER JOIN dbo.#tblLibrary l ON l.InstitutionCode=zt.InstitutionCode
INNER JOIN dbo.synSSDBx_tblDatabase d ON d.DatabaseCode=zt.DatabaseCode
--  exclude databases already added to dbo.#tblLibraryDatabase
LEFT JOIN dbo.#tblLibraryDatabase ld ON
	ld.LibraryId=l.LibraryId AND
	ld.DatabaseId=d.DatabaseId
WHERE ld.LibraryId IS NULL;

-----------------------------------------------
--Step 3) 360/ALMA_UNASSIGNED_HOLDINGS databases where LDTs=0  (not already included)
--			this favors an explicit 360 Zero Title Databases over the same database with no titles from processing the xml 
-----------------------------------------------
INSERT dbo.#tblLibraryDatabase (LibraryDatabaseId, LibraryId, DatabaseId, CustomDBURL, CustomDatabaseName, LinkAuthorization, IsSelect, OmitProxy, LibraryHash, DatabaseCode, Source) 
SELECT ld.LibraryDatabaseId, l.LibraryId, ld.DatabaseId, ld.CustomDBURL, ld.CustomDatabaseName, ld.LinkAuthorization, ld.IsSelect, ld.OmitProxy, l.LibraryHash, d.DatabaseCode, Source='Holdings'
FROM dbo.#tblLibrary l
INNER JOIN dbo.tblSF_LibraryDatabase ld ON 
	ld.LibraryId=l.LibraryId AND
	ld.LDTs=0
INNER JOIN dbo.synSSDBx_tblDatabase d ON d.DatabaseId=ld.DatabaseId
--  exclude databases already added to tblSOA_LibraryDatabase
LEFT JOIN dbo.#tblLibraryDatabase xld ON
	xld.LibraryId=l.LibraryId AND
	xld.DatabaseId=d.DatabaseId
WHERE xld.LibraryId IS NULL

-----------------------------------------------
--Step 4) Catalog, IR and LibGuides databases
-----------------------------------------------
INSERT dbo.#tblLibraryDatabase (LibraryDatabaseId, LibraryId, DatabaseId, CustomDBURL, CustomDatabaseName, LinkAuthorization, IsSelect, OmitProxy, LibraryHash, DatabaseCode, Source) 
SELECT ld.LibraryDatabaseId, l.LibraryId, ld.DatabaseId, CustomDBURL=NULL, CustomDatabaseName=NULL, LinkAuthorization=NULL, IsSelect=0, ld.OmitProxy, l.LibraryHash, d.DatabaseCode, d.Source
FROM dbo.#tblLibrary l
INNER JOIN dbo.tblAlmaInstitutionDatabase ld ON ld.LibraryId=l.LibraryId
INNER JOIN dbo.#tblDatabase d ON d.DatabaseId=ld.DatabaseId;

--SELECT * FROM dbo.#tblDatabase;
--SELECT * FROM dbo.#tblLibrary;
--SELECT * FROM dbo.#tblLibraryDatabase;

-------------------------------------------------------------------------------------------------------------------
-- return data
-------------------------------------------------------------------------------------------------------------------

-------------------------------------------------
-- Library
-------------------------------------------------
SELECT LibraryId,LibraryHash,LibraryName,Institution,ProxyURL=NULL,ProxyType=NULL
FROM dbo.#tblLibrary;

-------------------------------------------------
-- Databases
-------------------------------------------------
SELECT
	DatabaseId, 
	DatabaseCode, 
	DatabaseName, 
	ProvIderId, 
	TitleURLDefault=NULL,
	IsFullText,
	JournalLinkCoverage=NULL,
	ArticleLinkCoverage=NULL,
	MediaType=NULL, 
    ProviderCode=NULL, 
    ProviderName=NULL, 
    TitleURLDefault=NULL
FROM dbo.#tblDatabase;

-------------------------------------------------
-- Library Databases
-------------------------------------------------
-- per viewDataFeed_SOA_LibraryDatabase, DateStart and DateEnd and DateDisplay are all NULL
SELECT d.DatabaseCode, DateStart=NULL, DateEnd=NULL, DateDisplay=NULL, ld.IsSelect, d.IsFulltext, ld.LibraryId, ld.LibraryDatabaseId, ld.LibraryHash, ld.OmitProxy, ld.LinkAuthorization
FROM dbo.#tblLibraryDatabase ld
INNER JOIN (
	SELECT DatabaseId,DatabaseCode,IsFulltext FROM dbo.#tblDatabase
	UNION ALL
	SELECT DatabaseId,DatabaseCode,IsFulltext=HasFullText FROM dbo.synSSDBx_tblDatabase
) d ON d.DatabaseId=ld.DatabaseId
WHERE 
	ld.LibraryId=@LibraryId AND
	-- per CDI-773
	ld.IsSelect=0
ORDER BY DatabaseCode;

SET @LibraryDatabases=@@ROWCOUNT;

PRINT CONCAT('Library Databases (IsSelect=0/False) : ', @LibraryDatabases);

-------------------------------------------------
-- Library Database Titles
-------------------------------------------------
-- this was used to QA
--SELECT ldt.LibraryDatabaseId,Monographs=COUNT(*) 
--FROM dbo.#tblLibraryDatabase ld
--INNER JOIN dbo.tblSF_LibraryDatabaseTitle_Monograph ldt ON ldt.LibraryDatabaseId=ld.LibraryDatabaseId
--INNER JOIN dbo.synSSDBx_tblTitleMap tm ON tm.SSID_Lookup=ldt.SSIdentifier
--WHERE ld.LibraryId=@LibraryId
--GROUP BY ldt.LibraryDatabaseId
--ORDER BY ldt.LibraryDatabaseId

--SELECT ldt.LibraryDatabaseId,Serials=COUNT(*) 
--FROM dbo.#tblLibraryDatabase ld
--INNER JOIN dbo.tblSF_LibraryDatabaseTitle_Serial ldt ON ldt.LibraryDatabaseId=ld.LibraryDatabaseId
--INNER JOIN dbo.synSSDBx_tblTitleMap tm ON tm.SSID_Lookup=ldt.SSIdentifier
--WHERE ld.LibraryId=@LibraryId
--GROUP BY ldt.LibraryDatabaseId
--ORDER BY ldt.LibraryDatabaseId

-------------------------------------------------
-- Library Database Titles - Monograph (see SSDBx viewDataFeed_SOA_LibraryDatabaseTitle_Monograph)
-------------------------------------------------
SELECT DISTINCT 
	LibraryId=@LibraryId,
	tm.SSIdentifier,
	ld.LibraryDatabaseId
FROM dbo.#tblLibraryDatabase ld
INNER JOIN dbo.tblSF_LibraryDatabaseTitle_Monograph ldt ON ldt.LibraryDatabaseId=ld.LibraryDatabaseId
INNER JOIN dbo.synSSDBx_tblTitleMap tm ON tm.SSID_Lookup=ldt.SSIdentifier
WHERE ld.LibraryId=@LibraryId;

SET @Monographs=@@ROWCOUNT;

PRINT CONCAT('Library Database Titles - Monographs : ',@Monographs);

-------------------------------------------------
-- Library Database Titles - Serial (see SSDBx viewDataFeed_SOA_LibraryDatabaseTitle_Serial)
-------------------------------------------------
SELECT 
	tm.SSIdentifier,
	d.DatabaseCode,
	ld.DatabaseId,
	ds.DateStart,
	de.DateEnd, 
	de.DateDisplay, 
	d.IsFullText,
	LibraryId=@LibraryId,
	ld.LibraryDatabaseId
FROM dbo.#tblLibraryDatabase ld
INNER JOIN dbo.tblSF_LibraryDatabaseTitle_Serial ldt ON ldt.LibraryDatabaseId=ld.LibraryDatabaseId
INNER JOIN dbo.synSSDBx_tblTitleMap tm ON tm.SSID_Lookup=ldt.SSIdentifier
INNER JOIN (
	SELECT DatabaseId,DatabaseCode,IsFullText=HasFullText FROM dbo.synSSDBx_tblDatabase
	UNION
	SELECT DatabaseId,DatabaseCode,IsFullText=1 FROM dbo.tblSF_Database
) d ON d.DatabaseId=ld.DatabaseId
INNER JOIN dbo.synSSDBx_tblDate ds ON ds.DateId=ldt.DateStartId
INNER JOIN dbo.synSSDBx_tblDate de ON de.DateId=ldt.DateEndId
WHERE ld.LibraryId=@LibraryId;

SET @Serials=@@ROWCOUNT;

PRINT CONCAT('Library Database Titles - Serials : ',@Serials);

-------------------------------------------------
-- Wrap Up
-------------------------------------------------
SET @TimeEnd=GETDATE();
--SET @DurationSec=DATEDIFF(second,@TimeStart,@TimeEnd)

UPDATE dbo.tblxCDI_LibraryRightsLog SET
	LibraryDatabases=@LibraryDatabases,
	Monographs=@Monographs,
	Serials=@Serials,
	TimeEnd=@TimeEnd,
	DurationSec=DATEDIFF(second,TimeStart,@TimeEnd)
WHERE CDI_LibraryRightsLogId=@CDI_LibraryRightsLogId;

-----------------------------------------------------------------------------------------------------	
END TRY
BEGIN CATCH	
-----------------------------------------------------------------------------------------------------	
	DECLARE
		@CDI_AppErrorId int,
		@ErrorSeverity int, -- the user-defined severity level associated with this message
		@ErrorState int		-- If the same user-defined error is raised at multiple locations, using a unique ErrorState number for each location can help find which section of code is raising the errors.

	SET @ReturnCode=-1;

	-- rollback any open transactions
	IF XACT_STATE()<>0 BEGIN
		ROLLBACK TRAN;
		PRINT 'Transaction rolled back.';
	END

	SET @ErrorMessage=ERROR_MESSAGE();
	SET @ErrorSeverity=ERROR_SEVERITY();
	SET @ErrorState=ERROR_STATE();
	
	INSERT dbo.tblxCDI_AppError (DatabaseServer,DatabaseName,AppName,UserName,ErrorProcedure,ErrorLine,ErrorNumber,ErrorMessage,ErrorSeverity,ErrorState)
	SELECT 
		DatabaseServer=MetaDB.dbo.udfDBServerInstance(),
		DatabaseName=DB_NAME(),
		AppName=APP_NAME(),
		UserName=SUSER_NAME(),
		ErrorProcedure=ERROR_PROCEDURE(),
		ErrorLine=ERROR_LINE(),
		ErrorNumber=ERROR_NUMBER(),
		ErrorMessage=@ErrorMessage,
		ErrorSeverity=@ErrorSeverity,
		ErrorState=@ErrorState

	SET @CDI_AppErrorId=SCOPE_IDENTITY();

	UPDATE dbo.tblxCDI_LibraryRightsLog SET 
	CDI_AppErrorId=@CDI_AppErrorId
	WHERE CDI_LibraryRightsLogId=@CDI_LibraryRightsLogId;

	--SELECT * FROM dbo.tblxCDI_AppError WHERE CDI_AppErrorId=@CDI_AppErrorId;
	
	-- raise error to the client	
	RAISERROR (@ErrorMessage, @ErrorSeverity,@ErrorState);

-----------------------------------------------------------------------------------------------------	
END CATCH
-----------------------------------------------------------------------------------------------------	

RETURN @ReturnCode;


GO
