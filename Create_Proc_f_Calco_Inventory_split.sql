SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create PROCedure [etl].[create_f_Calco_Inventory_split] 

	@UniqueRunID NVARCHAR(100) = 'UNDEFINED',  
    @ParentPipelineID NVARCHAR(100) = 'UNDEFINED',  
    @PipelineID NVARCHAR(100) = 'UNDEFINED'
AS
BEGIN  
  
    DECLARE @RowsRead INT,  
            @RowsWritten INT,  
            @RowsUpdated INT,  
            @RowsDeleted INT,  
            @RowsByPassed INT,  
            @logid INT = 0,  
            @RowsAtTraget INT;  

	BEGIN TRY  
  
  
        /*Start the process*/  
        EXEC admin.usp_StartProcess @SystemName = 'Calco Refresh',  
                                    @SystemNameRank = 100,  
                                    @ParentPipelineID = @ParentPipelineID,  
                                    @UniqueRunID = @UniqueRunID,  
                                    @PipelineID = @PipelineID,  
                                    @ProcessName = 'etl.create_f_Calco_Inventory_split',  
                                    @SourceTableOrViewName = 'etl.v_f_calco_Inventory_split',  
                                    @TargetName = 'dwh.f_Inventory',  
                                    @logid = @logid OUTPUT ;



		DECLARE @Upsert TABLE  
        (  
            Change VARCHAR(30)  
        );
        SELECT @RowsRead = COUNT(1)  
        FROM  
        (SELECT NativeId FROM etl.v_f_calco_Inventory_split) AS p;  

MERGE dwh.f_Inventory t
USING [etl].v_f_calco_Inventory_split s
	ON t.NativeId = s.NativeId
	AND t.dpt_auto_key = 106
WHEN MATCHED THEN UPDATE
	SET 
	     t.PartId = s.PartId
	    ,t.[LotNumber] = s.[LotNumber]
	    ,t.VendorId = s.VendorId
	    ,t.LocationId = s.LocationId
	    ,t.OriginalLocationId = s.OriginalLocationId
	    ,t.WarehouseId = s.WarehouseId
	    ,t.OriginalWarehouseId = s.OriginalWarehouseId
	    ,t.EmployeeId = s.EmployeeId
	    ,t.AgeId = s.AgeId
	    ,t.NativeInvoiceLineId = s.NativeInvoiceLineId
	    ,t.NativePOLineId = s.NativePOLineId
	    ,t.PONumber = s.PONumber
	    ,t.ReceivedDate = s.ReceivedDate
	    ,t.QtyReceived = s.QtyReceived
	    ,t.QtyOnHand = s.QtyOnHand
	    ,t.QtyReserved = s.QtyReserved
	    ,t.QtyAvailable = s.QtyAvailable
	    ,t.UnitCost = s.UnitCost
	    ,t.UnitPrice = s.UnitPrice
	    ,t.CurrentCost = s.CurrentCost
	    ,t.CurrentValue = s.CurrentValue
	    ,t.OriginalCost = s.OriginalCost
	    ,t.OriginalValue = s.OriginalValue
	    ,t.IsHistorical = s.IsHistorical
	    ,t.NativeModifiedDate = s.NativeModifiedDate
	    ,t.ETLModifiedDate = GETDATE() 
	    ,t.ManufacturerId = s.ManufacturerId
		,t.[PartConditionId] = s.[PartConditionId]
		,t.[TestRptCode] = s.[TestRptCode]
		,t.[CertNumber] = s.[CertNumber]
		,t.[Revision] = s.[Revision]
        ,t.dpt_auto_key = s.dpt_auto_key		
WHEN NOT MATCHED THEN
INSERT (
      NativeId
	,PartId
	,[LotNumber]
	,VendorId
	,LocationId
	,OriginalLocationId
	,WarehouseId
	,OriginalWarehouseId
	,EmployeeId
	,DivisionId
	,AgeId
	,NativeInvoiceLineId
	,NativePOLineId
	,PONumber
	,ReceivedDate
	,QtyReceived
	,QtyOnHand
	,QtyReserved
	,QtyAvailable
	,UnitCost
	,UnitPrice
	,CurrentCost
	,CurrentValue
	,OriginalCost
	,OriginalValue
	,IsHistorical
	,NativeModifiedDate
	,ETLImportDate
	,ManufacturerId
 	,[PartConditionId]
	,[TestRptCode]
	,[CertNumber]
	,[Revision]	
    ,dpt_auto_key
)
VALUES (
      s.NativeId
	,s.PartId
	,s.[LotNumber]
	,s.VendorId
	,s.LocationId
	,s.OriginalLocationId
	,s.WarehouseId
	,s.OriginalWarehouseId
	,s.EmployeeId
	,s.DivisionId
	,s.AgeId
	,s.NativeInvoiceLineId
	,s.NativePOLineId
	,s.PONumber
	,s.ReceivedDate
	,s.QtyReceived
	,s.QtyOnHand
	,s.QtyReserved
	,s.QtyAvailable
	,s.UnitCost
	,s.UnitPrice
	,s.CurrentCost
	,s.CurrentValue
	,s.OriginalCost
	,s.OriginalValue
	,s.IsHistorical
	,s.NativeModifiedDate
	,s.ETLImportDate
	,s.ManufacturerId
	,s.[PartConditionId]
	,s.[TestRptCode]
	,s.[CertNumber]
	,s.[Revision]
    ,s.dpt_auto_key

)

OUTPUT $action  
INTO @Upsert;


--Adding Reconciliation data   
        SELECT @RowsWritten = ISNULL(SUM(CASE  WHEN Change = 'INSERT' THEN  1 ELSE 0 END), 0),  
               @RowsUpdated = ISNULL(SUM(CASE  WHEN Change = 'UPDATE' THEN 1 ELSE 0 END), 0)  
        FROM @Upsert;


		SELECT @RowsByPassed = @RowsRead - @RowsUpdated - @RowsWritten ;
  
        EXEC admin.usp_EndProcess @logid = @logid,  
                                  @ProcessStatus = 'PASS',  
                                  @ErrorDescription = NULL,  
                                  @RowsRead = @RowsRead,  
                                  @RowsWritten = @RowsWritten,  
                                  @RowsUpdated = @RowsUpdated,  
                                  @RowsByPassed = @RowsByPassed;  
  
    END TRY  
    BEGIN CATCH  
  
        DECLARE @ErrorDescription NVARCHAR(MAX);  
        SELECT @ErrorDescription = ERROR_MESSAGE();  
  
        EXEC admin.usp_EndProcess @logid = @logid,  
                                  @ProcessStatus = 'FAIL',  
                                  @RowsRead = 0,  
                                  @RowsWritten = 0,  
                                  @RowsUpdated = 0,  
                                  @RowsByPassed = 0,  
                                  @ErrorDescription = @ErrorDescription;  
  
        THROW;  
  
    END CATCH;  
  
END;


GO
