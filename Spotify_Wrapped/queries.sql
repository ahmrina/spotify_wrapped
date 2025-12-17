-- enabling xp_cmdshell
USE master;
GO

EXECUTE sp_configure 'show advanced options', 1;
GO

RECONFIGURE;
GO

EXECUTE sp_configure 'xp_cmdshell', 1;
GO

RECONFIGURE;
GO

EXECUTE sp_configure 'show advanced options', 0;
GO

RECONFIGURE;
GO




-- procedure to read spotify data files
CREATE PROCEDURE IMPORT_SPOTIFY_DATA

@folder_path NVARCHAR(100),
@pattern NVARCHAR(100) = 'Streaming_History_Audio*.json',
@table NVARCHAR(100) = 'SPOTIFY_HISTORY_' 

AS BEGIN SET NOCOUNT ON;

CREATE TABLE #file_list ( FileName NVARCHAR(500));

CREATE TABLE #temp (

      date_played DATETIME2,
      seconds_played FLOAT,
      song_title NVARCHAR(100),
      artist_name NVARCHAR(100),
      album_title NVARCHAR(200),
      reason_start NVARCHAR(50),
      reason_end NVARCHAR(50),
      skipped NVARCHAR(50) );

DECLARE @cmd NVARCHAR(500);
SET @cmd = 'DIR "' + @folder_path + '\' + @pattern + '" /B';

PRINT(@pattern);
PRINT(@cmd);

INSERT INTO #file_list (FileName)
EXEC xp_cmdshell @cmd

PRINT('ran xp_cmdshell');

-- variables
DECLARE @curr_file NVARCHAR(500);
DECLARE @full_path NVARCHAR(500);
DECLARE @json_data NVARCHAR(MAX);
DECLARE @sql NVARCHAR(MAX);
DECLARE @table_name NVARCHAR(100);
DECLARE @table_idx INT = 1;

DECLARE file_cursor CURSOR FOR 
SELECT FileName FROM #file_list;

OPEN file_cursor;
FETCH NEXT FROM file_cursor INTO @curr_file;

WHILE @@FETCH_STATUS = 0

BEGIN -- begin reading and creating tables

SET @full_path = @folder_path + '\' + @curr_file;
SET @table_name = @table + CAST(@table_idx AS NVARCHAR(1));

PRINT (@table_name + ' processed');

BEGIN TRY

   SET @sql = N'
        
       DECLARE @json_data NVARCHAR(MAX);
       SELECT @json_data = BulkColumn
         FROM OPENROWSET(BULK ''' + @full_path + ''', SINGLE_CLOB) AS j;

       SELECT ts as date_played, 
       ms_played / 1000.0 as seconds_played,
       master_metadata_track_name as song_title,
       master_metadata_album_artist_name as artist_name,
       master_metadata_album_album_name as album_title,
       reason_start,
       reason_end,
       skipped
   INTO ' + QUOTENAME(@table_name) + N'
   FROM OPENJSON(@json_data)
   WITH (ts DATETIME2,
      ms_played INT,
      master_metadata_track_name NVARCHAR(500),
      master_metadata_album_artist_name NVARCHAR(500),
      master_metadata_album_album_name NVARCHAR(500),
      reason_start NVARCHAR(50),
      reason_end NVARCHAR(50),
      skipped NVARCHAR(50));
      ';

   PRINT('attempting to run @sql');

    EXEC sp_executesql @sql;

   PRINT(@table_name + ' is created');
   SET @table_idx = @table_idx + 1;

END TRY

BEGIN CATCH
    PRINT ERROR_MESSAGE();
END CATCH 

FETCH NEXT FROM file_cursor INTO @curr_file;


END -- ending reading + creating tables

CLOSE file_cursor;
DEALLOCATE file_cursor;
DROP TABLE #file_list;

END -- end process
----------------------------------------------------------------------------------

--  cleaning procedure
CREATE PROCEDURE CLEAN_DATA
  @table NVARCHAR(100) -- table passed 

  AS BEGIN

  DECLARE @sql NVARCHAR(MAX);

  SET @sql = N'
             DELETE FROM ' + QUOTENAME(@table) + N' 
             WHERE song_title IS NULL OR artist_name IS NULL or album_title IS NULL';
 
 EXEC sp_executesql @sql;
  
 END; -- end procedure
 -------------------------------------------------------------------------------------
 CREATE PROCEDURE MOST_LISTENED_TRACKS
 @table NVARCHAR(100),
 @play_count INT

 AS BEGIN

 DECLARE @sql NVARCHAR(MAX);

 SET @sql = N'
             SELECT song_title, COUNT(*) AS play_count FROM' + QUOTENAME(@table) + 
             N' GROUP BY song_title, artist_name HAVING COUNT(*)>=' + CAST(@play_count AS NVARCHAR(4)) + N' ORDER BY play_count DESC';
 
 EXEC sp_executesql @sql;

 END; -- end procedure
 ------------------------------------------------------------------
 CREATE PROCEDURE MOST_LISTENED_ALBUMS
 @table NVARCHAR(100),
 @play_count INT

 AS BEGIN

 DECLARE @sql NVARCHAR(MAX);

 SET @sql = N'
              SELECT album_title, artist_name, COUNT(*) AS play_count FROM' + QUOTENAME(@table) + 
              N'GROUP BY album_title, artist_name 
              HAVING COUNT(*) >=' + CAST(@play_count as NVARCHAR(4)) + N'AND COUNT(DISTINCT song_title) >= 5 ORDER BY play_count DESC';
 EXEC sp_executesql @sql;

 END; -- end procedure
 -----------------------------------------------------------------
 CREATE PROCEDURE MOST_LISTENED_ARTISTS
 @table NVARCHAR(100),
 @play_count INT

 AS BEGIN 

 DECLARE @sql NVARCHAR(MAX);

 SET @sql = N'
            SELECT artist_name, COUNT(*) as play_count FROM' + QUOTENAME(@table) + N'
            GROUP BY artist_name HAVING COUNT(*) >=' +  CAST(@play_count as NVARCHAR(4)) + N' ORDER BY play_count DESC';

 EXEC sp_executesql @sql;
 
 END;
 ----------------------------------------------------------------------
 CREATE PROCEDURE MOST_CLICKED_SONGS
 @table NVARCHAR(100),
 @play_count INT

 AS BEGIN 

 DECLARE @sql NVARCHAR(MAX);

 SET @sql = N'
             SELECT song_title, COUNT (*) as play_count FROM' + QUOTENAME(@table) + N' WHERE reason_start = ''clickrow''
             GROUP BY song_title HAVING COUNT(*) >=' + CAST(@play_count as NVARCHAR(4)) + N'
             ORDER BY play_count DESC';
 
 EXEC sp_executesql @sql;

 END;
--------------------------------------------------------------
CREATE PROCEDURE MOST_SKIPPED_SONGS
@table NVARCHAR(100),
@skip_count INT

AS BEGIN

DECLARE @sql NVARCHAR(MAX);

SET @sql = N'
             SELECT song_title, COUNT(*) as skip_count FROM ' + QUOTENAME(@table) + N' WHERE skipped = ''True''
             GROUP BY song_title HAVING COUNT(*) >=' + CAST(@skip_count AS NVARCHAR(4)) + N'ORDER BY skip_count DESC';

EXEC sp_executesql @sql;

END;
---------------------------------------------------------------
CREATE PROCEDURE TOP_MUSIC_DAY
@table NVARCHAR(100),
@count INT

AS BEGIN

DECLARE @sql NVARCHAR(MAX);

SET @sql = N' 
             WITH DailyStats AS (
    SELECT 
        CAST(date_played AS DATE) as play_date,
        COUNT(*) as songs_played,
        SUM(seconds_played) / 60.0 as total_minutes
    FROM' + QUOTENAME(@table) + N'
    GROUP BY CAST(date_played AS DATE)
    HAVING COUNT(*) >=' + CAST(@count as NVARCHAR(4)) + N'
),
SongCounts AS (
    SELECT 
        CAST(date_played AS DATE) as play_date,
        song_title,
        artist_name,
        COUNT(*) as times_played
    FROM' + QUOTENAME(@table) + N'
    GROUP BY CAST(date_played AS DATE), song_title, artist_name
),
TOP_SONG_OF_DAY AS (
    SELECT 
        play_date, song_title, artist_name, times_played,
        ROW_NUMBER() OVER (PARTITION BY play_date ORDER BY times_played DESC) as rank
    FROM SongCounts
)
SELECT 
    d.play_date,
    d.songs_played,
    d.total_minutes,
    t.song_title as top_song,
    t.artist_name as top_artist,
    t.times_played as top_song_plays
FROM DailyStats d
LEFT JOIN TOP_SONG_OF_DAY t ON d.play_date = t.play_date AND t.rank = 1
ORDER BY d.songs_played DESC';

EXEC sp_executesql @sql;

END;
----------------------------------------------------------------
CREATE PROCEDURE TOP_ARTIST_DAY
@table NVARCHAR(100),
@count INT

AS BEGIN

DECLARE @sql NVARCHAR(MAX);

SET @sql = N'WITH DAILY_STATS AS (
SELECT CAST(date_played AS DATE) as date_played,
       COUNT(DISTINCT artist_name) as num_artists,
       SUM(seconds_played) / 60.0 as total_played FROM' + QUOTENAME(@table) + N'
GROUP BY CAST(date_played AS DATE) HAVING COUNT(DISTINCT artist_name) >=' + CAST(@count as NVARCHAR(4)) + N'), /*errror in this line*/

TOP_ARTIST_PER_DAY AS (
 
SELECT artist_name as top_artist, CAST(date_played as DATE) as date_played, 
        COUNT(*) as play_count, SUM(seconds_played) / 60.0 as artists_mins, 
        ROW_NUMBER() OVER 
        (PARTITION BY CAST(date_played AS DATE) /*error in this line */
        ORDER BY COUNT(*) DESC, SUM(seconds_played) DESC) 
        as ARTIST_RANK_PER_DAY FROM' + QUOTENAME(@table) + N' GROUP BY CAST(date_played as DATE), artist_name )


SELECT d.date_played, d.num_artists, d.total_played, t.top_artist, t.play_count, t.artists_mins FROM DAILY_STATS d
LEFT JOIN TOP_ARTIST_PER_DAY as t on  d.date_played = t.date_played AND t.ARTIST_RANK_PER_DAY = 1
ORDER BY d.num_artists DESC';

EXEC sp_executesql @sql;

END;
----------------------------------------------------------------



-- read spotify data
EXEC IMPORT_SPOTIFY_DATA
@folder_path = 'C:\temp\data_2025',
@pattern = 'Streaming_History_Audio_*.json',
@table  = 'SPOTIFY_HISTORY_';

-- clean all tables from null data
DECLARE @i INT = 1;
WHILE @i <= 4 BEGIN
   DECLARE @table_name NVARCHAR(100) = 'SPOTIFY_HISTORY_' + CAST(@i as NVARCHAR(1));
   EXEC CLEAN_DATA @table = @table_name;
   SET @i = @i + 1;
END


-- extract info from different quarters (example - for quarter analysis)
EXEC MOST_SKIPPED_SONGS @table = SPOTIFY_HISTORY_1, @skip_count = 100;
EXEC MOST_SKIPPED_SONGS @table = SPOTIFY_HISTORY_2, @skip_count = 100;
EXEC MOST_SKIPPED_SONGS @table = SPOTIFY_HISTORY_3, @skip_count = 50;
EXEC MOST_SKIPPED_SONGS @table = SPOTIFY_HISTORY_4, @skip_count = 10;

-- merge all tables for a full year wrapped 
CREATE VIEW SPOTIFY_HISTORY_FULL_DATA AS
SELECT * FROM SPOTIFY_HISTORY_1
UNION ALL
SELECT * FROM SPOTIFY_HISTORY_2
UNION ALL
SELECT * FROM SPOTIFY_HISTORY_3
UNION ALL
SELECT * FROM SPOTIFY_HISTORY_4

-- get top tracks
EXEC MOST_LISTENED_TRACKS @table = SPOTIFY_HISTORY_FULL_DATA, @play_count = 200

-- get top albums
EXEC MOST_LISTENED_ALBUMS @table = SPOTIFY_HISTORY_FULL_DATA, @play_count = 200

-- get top artists
EXEC MOST_LISTENED_ARTISTS @table = SPOTIFY_HISTORY_FULL_DATA, @play_count = 600

-- get top 
EXEC MOST_CLICKED_SONGS @table = SPOTIFY_HISTORY_FULL_DATA, @play_count = 100

-- get most skipped 
EXEC MOST_SKIPPED_SONGS @table = SPOTIFY_HISTORY_FULL_DATA, @skip_count = 100

EXEC TOP_ARTIST_DAY @table = SPOTIFY_HISTORY_FULL_DATA, @count = 120 -- num of artists played

EXEC TOP_MUSIC_DAY @table = SPOTIFY_HISTORY_FULL_DATA, @count = 350 -- num of songs played






