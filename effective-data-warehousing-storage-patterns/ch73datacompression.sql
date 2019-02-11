
/*
Paul Nielsen
SQL Server 2008 Bible (Wiley, 2008)
www.SQLServerBible.com

Chapter 73 - Data Compression

developed using Katami CPT6 - March 2008
*/


-- Query to determine current compression settings for all objects in database
 
  select o.object_id, 
      S.name as [schema], 
      o.name as [Object], 
      I.index_id as Ix_id, 
      I.name as IxName, 
      I.type_desc as IxType, 
      P.partition_number as P_No, 
      P.data_compression_desc as Compression
    from sys.schemas as S
      join sys.objects as O
        on S.schema_id = O.schema_id 
      join sys.indexes as I
        on o.object_id = I.object_id 
      join sys.partitions as P
        on I.object_id = P.object_id
        and I.index_id= p.index_id
     where O.TYPE = 'U'
     order by [schema], [object], i.index_id

go --------------------------------------------------------------------------------------------

CREATE PROC db_compression_estimate 
as
set nocount on 

-- estimates the row and page compression gain for every object and index in the database
-- Paul Nielsen
-- www.SQLServerBible.com 
-- March 13, 2008

-- to do:
-- [ ] transaction error from insert...Exec sp_estimate_data_compression_savings
-- [ ] filter objects to only those eligible for compression 

  CREATE TABLE #ObjEst (
    PK int identity not null primary key,
    object_name varchar(250),
    schema_name varchar(250),
    index_id INT,
    partition_number int,
    size_with_current_compression_setting bigint,
    size_with_requested_compression_setting bigint,
    sample_size_with_current_compression_setting bigint,
    sample_size_with_requested_compresison_setting bigint
    )
    
  CREATE TABLE #dbEstimate (
    PK int identity not null primary key,
    schema_name varchar(250),
    object_name varchar(250),
    index_id INT,
    ixName VARCHAR(255),
    ixType VARCHAR(50),
    partition_number int,
    data_compression_desc VARCHAR(50),
    None_Size INT,
    Row_Size INT,
    Page_Size INT
    )
  
  INSERT INTO #dbEstimate (schema_name, object_name, index_id, ixName, ixType, partition_number, data_compression_desc)
      select S.name, o.name, I.index_id, I.name, I.type_desc, P.partition_number, P.data_compression_desc
        from sys.schemas as S
          join sys.objects as O
            on S.schema_id = O.schema_id 
          join sys.indexes as I
	          on o.object_id = I.object_id 
	        join sys.partitions as P
	          on I.object_id = P.object_id
	          and I.index_id= p.index_id
	       where O.TYPE = 'U' 
	       
 -- Determine Compression Estimates 
  DECLARE
    @PK INT,
    @Schema varchar(150),
    @object varchar(150),
    @DAD varchar(25),
    @partNO int,
    @indexID int,
    @SQL nVARCHAR(max)
 
  DECLARE cCompress CURSOR FAST_FORWARD
    FOR 
      select schema_name, object_name, index_id, partition_number, data_compression_desc
        FROM #dbEstimate
   
  OPEN cCompress
  
  FETCH cCompress INTO @Schema, @object, @indexID, @partNO, @DAD  -- prime the cursor
 
  WHILE @@Fetch_Status = 0 
    BEGIN
        
    IF @DAD = 'none'
      BEGIN 
            -- estimate Page compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
              EXEC sp_estimate_data_compression_savings
                @Schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'page'
                
             UPDATE #dbEstimate
                SET none_size = O.size_with_current_compression_setting,
                    page_size = O.size_with_requested_compression_setting
                FROM #dbEstimate D
                  JOIN #ObjEst O
                    ON  D.Schema_name = O.Schema_Name
                    and D.Object_name = O.object_name
                    and D.index_id = O.index_id
                    and D.partition_number = O.partition_number  
                    
             DELETE #ObjEst  
             
             -- estimate Row compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
              EXEC sp_estimate_data_compression_savings
                @Schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'row'
                
             UPDATE #dbEstimate
                SET row_size = O.size_with_requested_compression_setting
                FROM #dbEstimate D
                  JOIN #ObjEst O
                    ON  D.Schema_name = O.Schema_Name
                    and D.Object_name = O.object_name
                    and D.index_id = O.index_id
                    and D.partition_number = O.partition_number  
                    
             DELETE #ObjEst       
        END -- none compression estimate      
 
    IF @DAD = 'row'
      BEGIN 
            -- estimate Page compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
              EXEC sp_estimate_data_compression_savings
                @Schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'page'
                
             UPDATE #dbEstimate
                SET row_size = O.size_with_current_compression_setting,
                    page_size = O.size_with_requested_compression_setting
                FROM #dbEstimate D
                  JOIN #ObjEst O
                    ON  D.Schema_name = O.Schema_Name
                    and D.Object_name = O.object_name
                    and D.index_id = O.index_id
                    and D.partition_number = O.partition_number  
                    
             DELETE #ObjEst  
             
             -- estimate None compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
              EXEC sp_estimate_data_compression_savings
                @Schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'none'
                
             UPDATE #dbEstimate
                SET none_size = O.size_with_requested_compression_setting
                FROM #dbEstimate D
                  JOIN #ObjEst O
                    ON  D.Schema_name = O.Schema_Name
                    and D.Object_name = O.object_name
                    and D.index_id = O.index_id
                    and D.partition_number = O.partition_number  
                    
             DELETE #ObjEst       
        END -- row compression estimate     
      
    IF @DAD = 'page'
      BEGIN 
            -- estimate Row compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
              EXEC sp_estimate_data_compression_savings
                @Schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'row'
                
             UPDATE #dbEstimate
                SET page_size = O.size_with_current_compression_setting,
                    row_size = O.size_with_requested_compression_setting
                FROM #dbEstimate D
                  JOIN #ObjEst O
                    ON  D.Schema_name = O.Schema_Name
                    and D.Object_name = O.object_name
                    and D.index_id = O.index_id
                    and D.partition_number = O.partition_number  
                    
             DELETE #ObjEst  
             
             -- estimate None compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
              EXEC sp_estimate_data_compression_savings
                @Schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'none'
                
             UPDATE #dbEstimate
                SET none_size = O.size_with_requested_compression_setting
                FROM #dbEstimate D
                  JOIN #ObjEst O
                    ON  D.Schema_name = O.Schema_Name
                    and D.Object_name = O.object_name
                    and D.index_id = O.index_id
                    and D.partition_number = O.partition_number  
                    
             DELETE #ObjEst       
        END -- page compression estimate 
          
       FETCH cCompress INTO @Schema, @object, @indexID, @partNO, @DAD 
    END

  CLOSE cCompress
  DEALLOCATE cCompress
  
 -- report findings
select schema_name + '.' + object_name as [Object], index_id, ixName, ixType, partition_number,
     data_compression_desc as Current_Compression, 
     CAST((1-(cast(Row_Size as float) / none_Size))*100 as int)  as RowGain,
     CAST((1-(cast(page_Size as float) / none_Size))*100 as int) as PageGain,
     Case 
       when (1-(cast(Row_Size as float) / none_Size)) >= .25 and (Row_Size <= Page_Size) then 'Row' 
       when (1-(cast(page_Size as float) / none_Size)) >= .25 and (Page_Size <= row_Size) then 'Page' 
       else 'None' 
     end as Recommended_Compression
   from #dbEstimate 
   where None_Size <> 0
   order by [Object]
   
 Return 
 
go --------------------------------------------------------------------------------------------


CREATE PROC db_compression (
  @minCompression float = null -- e.g. .25 for minimum of 25% compression
  )
as
set nocount on 

-- Paul Nielsen
-- www.SQLServerBible.com 
-- March 13, 2008

/*
  sets compression for all objects and indexs in the database needing adjustment
  if estimated gain is equal to or greater than mincompression parameter
    then enables row or page compression whichever is greater gain
  if row and page have same gain
    then enables row compression
  if estimated gain is less than mincompression parameter 
    then compression is set to none
    
*/

-- to do:
-- [ ] transaction error from insert...Exec sp_estimate_data_compression_savings
-- [ ] filter objects to only those eligible for compression 


  IF @minCompression is null SET @minCompression = .25

  CREATE TABLE #ObjEst (
    PK int identity not null primary key,
    object_name varchar(250),
    schema_name varchar(250),
    index_id INT,
    partition_number int,
    size_with_current_compression_setting bigint,
    size_with_requested_compression_setting bigint,
    sample_size_with_current_compression_setting bigint,
    sample_size_with_requested_compresison_setting bigint
    )
    
  CREATE TABLE #dbEstimate (
    PK int identity not null primary key,
    schema_name varchar(250),
    object_name varchar(250),
    index_id INT,
    ixName VARCHAR(255),
    ixType VARCHAR(50),
    partition_number int,
    data_compression_desc VARCHAR(50),
    None_Size INT,
    Row_Size INT,
    Page_Size INT
    )
  
  INSERT INTO #dbEstimate (schema_name, object_name, index_id, ixName, ixType, partition_number, data_compression_desc)
      select S.name, o.name, I.index_id, I.name, I.type_desc, P.partition_number, P.data_compression_desc
        from sys.schemas as S
          join sys.objects as O
            on S.schema_id = O.schema_id 
          join sys.indexes as I
	          on o.object_id = I.object_id 
	        join sys.partitions as P
	          on I.object_id = P.object_id
	          and I.index_id= p.index_id
	       where O.TYPE = 'U' 
	       
 -- Determine Compression Estimates 
  DECLARE
    @PK INT,
    @Schema varchar(150),
    @object varchar(150),
    @DAD varchar(25),
    @partNO int,
    @indexID int,
    @ixName VARCHAR(250),
    @SQL nVARCHAR(max),
    @ixType VARCHAR(50), 
    @Recommended_Compression VARCHAR(10)
    
 
  DECLARE cCompress CURSOR FAST_FORWARD
    FOR 
      select schema_name, object_name, index_id, partition_number, data_compression_desc
        FROM #dbEstimate
   
  OPEN cCompress
  
  FETCH cCompress INTO @Schema, @object, @indexID, @partNO, @DAD  -- prime the cursor
 
  WHILE @@Fetch_Status = 0 
    BEGIN
        
    IF @DAD = 'none'
      BEGIN 
            -- estimate Page compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
              EXEC sp_estimate_data_compression_savings
                @Schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'page'
                
             UPDATE #dbEstimate
                SET none_size = O.size_with_current_compression_setting,
                    page_size = O.size_with_requested_compression_setting
                FROM #dbEstimate D
                  JOIN #ObjEst O
                    ON  D.Schema_name = O.Schema_Name
                    and D.Object_name = O.object_name
                    and D.index_id = O.index_id
                    and D.partition_number = O.partition_number  
                    
             DELETE #ObjEst  
             
             -- estimate Row compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
              EXEC sp_estimate_data_compression_savings
                @Schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'row'
                
             UPDATE #dbEstimate
                SET row_size = O.size_with_requested_compression_setting
                FROM #dbEstimate D
                  JOIN #ObjEst O
                    ON  D.Schema_name = O.Schema_Name
                    and D.Object_name = O.object_name
                    and D.index_id = O.index_id
                    and D.partition_number = O.partition_number  
                    
             DELETE #ObjEst       
        END -- none compression estimate      
 
    IF @DAD = 'row'
      BEGIN 
            -- estimate Page compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
              EXEC sp_estimate_data_compression_savings
                @Schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'page'
                
             UPDATE #dbEstimate
                SET row_size = O.size_with_current_compression_setting,
                    page_size = O.size_with_requested_compression_setting
                FROM #dbEstimate D
                  JOIN #ObjEst O
                    ON  D.Schema_name = O.Schema_Name
                    and D.Object_name = O.object_name
                    and D.index_id = O.index_id
                    and D.partition_number = O.partition_number  
                    
             DELETE #ObjEst  
             
             -- estimate None compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
              EXEC sp_estimate_data_compression_savings
                @Schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'none'
                
             UPDATE #dbEstimate
                SET none_size = O.size_with_requested_compression_setting
                FROM #dbEstimate D
                  JOIN #ObjEst O
                    ON  D.Schema_name = O.Schema_Name
                    and D.Object_name = O.object_name
                    and D.index_id = O.index_id
                    and D.partition_number = O.partition_number  
                    
             DELETE #ObjEst       
        END -- row compression estimate     
      
    IF @DAD = 'page'
      BEGIN 
            -- estimate Row compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
              EXEC sp_estimate_data_compression_savings
                @Schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'row'
                
             UPDATE #dbEstimate
                SET page_size = O.size_with_current_compression_setting,
                    row_size = O.size_with_requested_compression_setting
                FROM #dbEstimate D
                  JOIN #ObjEst O
                    ON  D.Schema_name = O.Schema_Name
                    and D.Object_name = O.object_name
                    and D.index_id = O.index_id
                    and D.partition_number = O.partition_number  
                    
             DELETE #ObjEst  
             
             -- estimate None compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
              EXEC sp_estimate_data_compression_savings
                @Schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'none'
                
             UPDATE #dbEstimate
                SET none_size = O.size_with_requested_compression_setting
                FROM #dbEstimate D
                  JOIN #ObjEst O
                    ON  D.Schema_name = O.Schema_Name
                    and D.Object_name = O.object_name
                    and D.index_id = O.index_id
                    and D.partition_number = O.partition_number  
                    
             DELETE #ObjEst       
        END -- page compression estimate 
          
       FETCH cCompress INTO @Schema, @object, @indexID, @partNO, @DAD 
    END

  CLOSE cCompress
  DEALLOCATE cCompress
  
   
 -- set the compression 
 DECLARE cCompress CURSOR FAST_FORWARD
    FOR 
      select schema_name, object_name, partition_number, ixName, ixType,  
         Case 
           when (1-(cast(Row_Size as float) / none_Size)) >= @minCompression and (Row_Size <= Page_Size) then 'Row' 
           when (1-(cast(page_Size as float) / none_Size)) >= @minCompression and (Page_Size <= row_Size) then 'Page' 
           else 'None' 
         end as Recommended_Compression
       from #dbEstimate 
       where None_Size <> 0
       and (Case 
           when (1-(cast(Row_Size as float) / none_Size)) >= @minCompression and (Row_Size <= Page_Size) then 'Row' 
           when (1-(cast(page_Size as float) / none_Size)) >= @minCompression and (Page_Size <= row_Size) then 'Page' 
           else 'None' 
         end 
         <> data_compression_desc)
   
  OPEN cCompress
  
  FETCH cCompress INTO @Schema, @object, @partNO, @ixName, @ixType, @Recommended_Compression  -- prime the cursor
 
  WHILE @@Fetch_Status = 0 
    BEGIN
    
    
      IF @ixType = 'Clustered' or @ixType='heap'
      set @SQL = 'ALTER TABLE ' + @schema + '.' + @object + ' Rebuild with (data_compression = ' + @Recommended_Compression + ')'
      
      else 
      set @SQL = 'ALTER INDEX ' + @ixName + ' on ' + @schema + '.' + @object + ' Rebuild with (data_compression = ' + @Recommended_Compression + ')'

      print @SQL        
      
      EXEC sp_executesql @SQL
      
      FETCH cCompress INTO @Schema, @object, @partNO, @ixName, @ixType, @Recommended_Compression  -- prime the cursor
    END

  CLOSE cCompress
  DEALLOCATE cCompress
  
  RETURN
  
  
  
   
