SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*  
Author: Landon Ochs   
Date: 8/12/19  
Called By: SSIS Package - Calco ETL  
Purpose: Keeps the data warehouse tables up to date with the source  
=======================================================  
Notes:  08-08-2023 Partap Singh Added UOM Column
11-01-2024 Wayne Chia fixed the MERGE conditions to work, Anu did the Rank for AHE Migration at the last part of the SP  
*/  
CREATE PROCedure [etl].[create_f_Calco_PurchaseOrder_split]   
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
                                    @ProcessName = 'etl.create_f_Calco_PurchaseOrder_split',    
                                    @SourceTableOrViewName = 'etl.v_f_calco_PurchaseOrder_split',    
                                    @TargetName = 'dwh.f_PurchaseOrder',    
                                    @logid = @logid OUTPUT ;  
  
  
  
  DECLARE @Upsert TABLE    
        (    
            Change VARCHAR(30)    
        );  
        SELECT @RowsRead = COUNT(1)    
        FROM    
        (SELECT NativeId FROM etl.v_f_calco_PurchaseOrder_split) AS p;    

DROP TABLE IF EXISTS #TEMPCalco
SELECT * INTO #TEMPCalco FROM etl.v_f_calco_PurchaseOrder_split WHERE DivisionId=12
  
MERGE dwh.f_PurchaseOrder t  
USING #TEMPCalco s
 ON t.NativeId = s.NativeId  
 AND t.NativeLineId = s.NativeLineId  
 AND t.DivisionId = 1  AND s.DivisionId=12
 AND t.DivisionId = s.DivisionId
WHEN MATCHED THEN UPDATE  
 SET   
      t.VendorId = s.VendorId  
     ,t.EmployeeId = s.EmployeeId  
     ,t.CustomerId = s.CustomerId  
     ,t.WarehouseId = s.WarehouseId  
     ,t.PartId = s.PartId  
     ,t.AltPartId = s.AltPartId  
     ,t.RouteId = s.RouteId  
     ,t.DPT_AUTO_KEY = s.DPT_AUTO_KEY
     ,t.PONumber = s.PONumber  
     ,t.LineNumber = s.LineNumber  
     ,t.PODate = s.PODate  
     ,t.PODateId = s.PODateId  
     ,t.ShipDate = s.ShipDate  
     ,t.ETADate = s.ETADate  
     ,t.DeliveryDate = s.DeliveryDate  
     ,t.QtyBackOrdered = s.QtyBackOrdered  
     ,t.QtyOrdered = s.QtyOrdered  
     ,t.QtyReceived = s.QtyReceived  
     ,t.QtyOpen = s.QtyOpen  
     ,t.UnitCost = s.UnitCost  
     ,t.LineCost = s.LineCost  
     ,t.VendorPrice = s.VendorPrice  
     ,t.IsOpen = s.IsOpen  
     ,t.IsResale = s.IsResale  
     ,t.IsDropShip = s.IsDropShip  
     ,t.[SO_DEPT_NAME] = s.[SO_DEPT_NAME]
     ,t.[PO_DEPT_NAME] = s.[PO_DEPT_NAME]
     ,t.[ACCOUNT_TYPE_DESC] = s.[ACCOUNT_TYPE_DESC]
     ,t.[DESCRIPTION] = s.[DESCRIPTION]
     ,t.[warehouse] = s.[warehouse]
     ,t.NativeModifiedDate = s.NativeModifiedDate  
     ,t.NativeModifiedDateLine = s.NativeModifiedDateLine  
     ,t.ETLModifiedDate = GETDATE() 
	 ,t.UoM = s.UoM
WHEN NOT MATCHED THEN  
INSERT (  
      NativeId  
 ,NativeLineId  
 ,VendorId  
 ,EmployeeId  
 ,CustomerId  
 ,WarehouseId  
 ,PartId  
 ,AltPartId  
 ,RouteId  
 ,DPT_AUTO_KEY
 ,DivisionId  
 ,PONumber  
 ,LineNumber  
 ,PODate  
 ,PODateId  
 ,ShipDate  
 ,ETADate  
 ,DeliveryDate  
 ,QtyBackOrdered  
 ,QtyOrdered  
 ,QtyReceived  
 ,QtyOpen  
 ,UnitCost  
 ,LineCost  
 ,VendorPrice  
 ,IsOpen  
 ,IsResale  
 ,IsDropShip  
 ,[SO_DEPT_NAME]
 ,[PO_DEPT_NAME]
 ,[ACCOUNT_TYPE_DESC]
 ,[DESCRIPTION]
 ,[warehouse]
 ,NativeModifiedDate  
 ,NativeModifiedDateLine  
 ,ETLImportDate  
 ,UoM  
)  
VALUES (  
      s.NativeId  
 ,s.NativeLineId  
 ,s.VendorId  
 ,s.EmployeeId  
 ,s.CustomerId  
 ,s.WarehouseId  
 ,s.PartId  
 ,s.AltPartId  
 ,s.RouteId  
 ,s.DPT_AUTO_KEY
 ,s.DivisionId  
 ,s.PONumber  
 ,s.LineNumber  
 ,s.PODate  
 ,s.PODateId  
 ,s.ShipDate  
 ,s.ETADate  
 ,s.DeliveryDate  
 ,s.QtyBackOrdered  
 ,s.QtyOrdered  
 ,s.QtyReceived  
 ,s.QtyOpen  
 ,s.UnitCost  
 ,s.LineCost  
 ,s.VendorPrice  
 ,s.IsOpen  
 ,s.IsResale  
 ,s.IsDropShip  
 ,s.[SO_DEPT_NAME]
 ,s.[PO_DEPT_NAME]
 ,s.[ACCOUNT_TYPE_DESC]
 ,s.[DESCRIPTION]
 ,s.[warehouse]
 ,s.NativeModifiedDate  
 ,s.NativeModifiedDateLine  
 ,s.ETLImportDate 
 ,s.UoM
  
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
