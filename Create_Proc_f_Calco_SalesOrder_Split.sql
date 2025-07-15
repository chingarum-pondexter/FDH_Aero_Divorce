SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



/*
Author: Landon Ochs 
Date: 8/14/19
Called By: SSIS Package - Calco ETL
Purpose: Keeps the data warehouse tables up to date with the source
=======================================================
Notes:
11/8/2021 J. Burns - Added [Status] column
*/
Create PROCedure [etl].[create_f_Calco_SalesOrder_Split] 
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
                                    @ProcessName = 'etl.create_f_Calco_SalesOrder_Split',  
                                    @SourceTableOrViewName = 'etl.v_f_calco_SalesOrder_split',  
                                    @TargetName = 'dwh.f_SalesOrder',  
                                    @logid = @logid OUTPUT ;



		DECLARE @Upsert TABLE  
        (  
            Change VARCHAR(30)  
        );
        SELECT @RowsRead = COUNT(1)  
        FROM  
        (SELECT NativeId FROM etl.v_f_calco_SalesOrder_split) AS p;  

DROP TABLE IF EXISTS #TEMPCalco
SELECT * INTO #TEMPCalco FROM etl.v_f_calco_SalesOrder_split WHERE DivisionId=12

MERGE dwh.f_SalesOrder t
USING #TEMPCalco s
	ON t.NativeId = s.NativeId
	AND t.NativeLineId = s.NativeLineId
	AND t.DivisionId=12
	AND S.DivisionId=12
WHEN MATCHED THEN UPDATE
	SET 
	     t.CustomerId = s.CustomerId
	    ,t.SalesRepId = s.SalesRepId
	    ,t.PartId = s.PartId
	    ,t.AltPartId = s.AltPartId
	    ,t.EmployeeId = s.EmployeeId
	    ,t.WarehouseId = s.WarehouseId
	    ,t.RouteId = s.RouteId
	    ,t.SONumber = s.SONumber
	    ,t.LineNumber = s.LineNumber
	    ,t.NativeQuoteLineId = s.NativeQuoteLineId
	    ,t.OrderDate = s.OrderDate
	    ,t.OrderDateId = s.OrderDateId
	    ,t.ShipDate = s.ShipDate
	    ,t.ShipDateId = s.ShipDateId
	    ,t.DueDate = s.DueDate
	    ,t.DeliveryDate = s.DeliveryDate
	    ,t.UnitPrice = s.UnitPrice
	    ,t.UnitCost = s.UnitCost
	    ,t.LinePrice = s.LinePrice
	    ,t.LineCost = s.LineCost
	    ,t.Discount = s.Discount
	    ,t.QtyOrdered = s.QtyOrdered
	    ,t.QtyShipped = s.QtyShipped
	    ,t.QtyInvoiced = s.QtyInvoiced
	    ,t.QtyOpen = s.QtyOpen
	    ,t.QtyAllocated = s.QtyAllocated
	    ,t.ExchangeRate = s.ExchangeRate
	    ,t.IsOpen = s.IsOpen
	    ,t.NativeModifiedDate = s.NativeModifiedDate
	    ,t.NativeModifiedDateLine = s.NativeModifiedDateLine
	    ,t.BillingZipCode = s.BillingZipCode
	    ,t.BillingCountry = s.BillingCountry
	    ,t.ETLModifiedDate = GETDATE() 
	    ,t.IsLTA = s.IsLTA
	    ,t.ProductClass = s.ProductClass
        ,t.Status = s.Status
		,t.Contract_Number=s.CONTRACT_NUMBER
        ,t.dpt_auto_key = s.dpt_auto_key
WHEN NOT MATCHED THEN
INSERT (
      NativeId
	,NativeLineId
	,CustomerId
	,SalesRepId
	,PartId
	,AltPartId
	,EmployeeId
	,WarehouseId
	,RouteId
	,DivisionId
	,SONumber
	,LineNumber
	,NativeQuoteLineId
	,OrderDate
	,OrderDateId
	,ShipDate
	,ShipDateId
	,DueDate
	,DeliveryDate
	,UnitPrice
	,UnitCost
	,LinePrice
	,LineCost
	,Discount
	,QtyOrdered
	,QtyShipped
	,QtyInvoiced
	,QtyOpen
	,QtyAllocated
	,ExchangeRate
	,IsOpen
	,NativeModifiedDate
	,NativeModifiedDateLine
	,BillingZipCode
	,BillingCountry
	,ETLImportDate
	,IsLTA
	,ProductClass
    ,Status
	,Contract_Number
    ,dpt_auto_key
)
VALUES (
      s.NativeId
	,s.NativeLineId
	,s.CustomerId
	,s.SalesRepId
	,s.PartId
	,s.AltPartId
	,s.EmployeeId
	,s.WarehouseId
	,s.RouteId
	,s.DivisionId
	,s.SONumber
	,s.LineNumber
	,s.NativeQuoteLineId
	,s.OrderDate
	,s.OrderDateId
	,s.ShipDate
	,s.ShipDateId
	,s.DueDate
	,s.DeliveryDate
	,s.UnitPrice
	,s.UnitCost
	,s.LinePrice
	,s.LineCost
	,s.Discount
	,s.QtyOrdered
	,s.QtyShipped
	,s.QtyInvoiced
	,s.QtyOpen
	,s.QtyAllocated
	,s.ExchangeRate
	,s.IsOpen
	,s.NativeModifiedDate
	,s.NativeModifiedDateLine
	,s.BillingZipCode
	,s.BillingCountry
	,s.ETLImportDate
	,s.IsLTA
	,s.ProductClass
    ,s.Status
    ,S.CONTRACT_NUMBER
    ,S.dpt_auto_key
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
  
    END CATCH
  
END;


GO
