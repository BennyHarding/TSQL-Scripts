-- Sample script to create Data Masking rules based on classification information
-- NOT WORKING AND FULLY TESTED.
-- RECOMMEND TO RUN IN @debug = 1
-- Need to exclude columns that are used for distribution in SQLDW.

IF OBJECT_ID('dbo.usp_create_defaultdynamicdatamasking_from_classification') IS NOT NULL
	DROP PROCEDURE dbo.usp_create_defaultdynamicdatamasking_from_classification
GO

CREATE PROCEDURE dbo.usp_create_defaultdynamicdatamasking_from_classification
(
	@debug	BIT = 1
)
AS

SET NOCOUNT ON;

DECLARE @sqlcmd		NVARCHAR(MAX)
DECLARE @maxrownum	BIGINT
DECLARE @rowcounter	BIGINT

IF OBJECT_ID('tempdb..#classifiedcolumns') IS NOT NULL
BEGIN
    DROP TABLE #classifiedcolumns
END

CREATE TABLE #classifiedcolumns WITH(DISTRIBUTION = ROUND_ROBIN) AS
SELECT
	ROW_NUMBER() OVER (ORDER BY o.[object_id], c.[column_id]) AS rownum
	, CAST(s.[name] AS NVARCHAR(128)) AS schemaname
	, CAST(o.[name] AS NVARCHAR(128)) AS tablename
	, CAST(c.[name] AS NVARCHAR(128)) AS columnname
	, CAST(COALESCE(TYPE_NAME(c.[system_type_id]), TYPE_NAME(c.[user_type_id])) AS NVARCHAR(128)) AS datatype
	, c.[max_length] AS max_length
	, c.[precision]AS precisionx
	, c.[scale] AS scale
	, CONVERT(NVARCHAR(128), sc.[label]) AS label_desc
	, CAST(sc.[information_type] AS NVARCHAR(128)) AS informationtype
FROM
	sys.sensitivity_classifications AS sc
	INNER JOIN sys.objects AS o
	ON sc.major_id = o.object_id
	INNER JOIN sys.schemas AS s
	ON o.schema_id = s.schema_id
	INNER JOIN sys.all_columns AS c
	ON o.object_id = c.object_id
	AND sc.minor_id = c.column_id

SELECT * FROM #classifiedcolumns ORDER BY rownum ASC

SET @rowcounter = 0
SET @maxrownum = (SELECT MAX(rownum) FROM #classifiedcolumns)

WHILE @rowcounter < @maxrownum
BEGIN
	SET @rowcounter = @rowcounter + 1

	SELECT @sqlcmd = N'ALTER TABLE [' + cc.[schemaname] + N'].[' + cc.[tablename] + N']' + NCHAR(13)
	  + N'ALTER COLUMN [' + cc.[columnname] + N'] ' + cc.datatype
	  + CASE cc.datatype
		WHEN 'char' THEN N'(' + CAST(cc.max_length AS NVARCHAR(20)) + N') '
		WHEN 'varchar' THEN N'(' + CASE cc.max_length WHEN -1 THEN 'max' ELSE  + CAST(cc.max_length AS NVARCHAR(20))  END + N') '
		WHEN 'nvarchar' THEN N'(' + CASE cc.max_length WHEN -1 THEN 'max' ELSE  + CAST(cc.max_length/2 AS NVARCHAR(20))  END + N') '
		WHEN 'nchar' THEN N'(' + CAST(cc.max_length/2 AS NVARCHAR(20)) + N') '
		WHEN 'decimal' THEN N'(' + CAST(cc.precisionx AS NVARCHAR(20)) + N',' + CAST(cc.scale AS NVARCHAR(20)) + N') '
		WHEN 'float' THEN N'(' + CAST(cc.precisionx AS NVARCHAR(20)) + N') '
		WHEN 'datetime2' THEN N'(' + CAST(cc.precisionx AS NVARCHAR(20)) + N') '
		WHEN 'datetimeoffset' THEN N'(' + CAST(cc.precisionx AS NVARCHAR(20)) + N') '
		WHEN 'time' THEN N'(' + CAST(cc.precisionx AS NVARCHAR(20)) + N') '
		ELSE ' '
	END
	+ N'MASKED WITH (FUNCTION = ''default()'');' + NCHAR(13) + NCHAR(13)
	FROM
		#classifiedcolumns AS cc
	WHERE
		cc.rownum = @rowcounter

	PRINT @sqlcmd

	IF @debug <> 1
		EXEC sp_executesql @sqlcmd

END