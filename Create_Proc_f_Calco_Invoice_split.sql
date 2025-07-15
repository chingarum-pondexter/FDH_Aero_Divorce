SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


Create PROCedure [etl].[create_f_Calco_Invoice_split] 
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
                                    @ProcessName = 'etl.create_f_Calco_Invoice_split',  
                                    @SourceTableOrViewName = 'etl.v_f_calco_Invoice_split',  
                                    @TargetName = 'dwh.f_Invoice',  
                                    @logid = @logid OUTPUT ;



		DECLARE @Upsert TABLE  
        (  
            Change VARCHAR(30)  
        );
        SELECT @RowsRead = COUNT(1)  
        FROM  
        (SELECT NativeId FROM etl.v_f_calco_Invoice_split) AS p;  
--Invoice currently has no delta logic available. If it becomes available remove the truncate statement
DELETE dwh.f_Invoice
WHERE dpt_auto_key = 106;

MERGE dwh.f_Invoice t
USING [etl].v_f_calco_Invoice_split s
	ON t.NativeId = s.NativeId
	AND t.NativeLineId = s.NativeLineId
	AND t.DivisionId = 12
WHEN MATCHED THEN UPDATE
	SET 
	     t.EmployeeId = s.EmployeeId
	    ,t.CustomerId = s.CustomerId
	    ,t.PartId = s.PartId
	    ,t.SalesRepId = s.SalesRepId
	    ,t.RouteId = s.RouteId
	    ,t.InvoiceNumber = s.InvoiceNumber
	    ,t.LineNumber = s.LineNumber
	    ,t.NativeSOLineId = s.NativeSOLineId
	    ,t.InvoiceDate = s.InvoiceDate
	    ,t.InvoiceDateId = s.InvoiceDateId
	    ,t.OrderDate = s.OrderDate
	    ,t.OrderDateId = s.OrderDateId
	    ,t.ShipDate = s.ShipDate
	    ,t.PostDate = s.PostDate
	    ,t.PostStatus = s.PostStatus
	    ,t.InvoiceType = s.InvoiceType
	    ,t.ExchangeRate = s.ExchangeRate
	    ,t.QtyShipped = s.QtyShipped
	    ,t.QtyBackOrdered = s.QtyBackOrdered
	    ,t.UnitCost = s.UnitCost
	    ,t.UnitPrice = s.UnitPrice
	    ,t.LinePrice = s.LinePrice
	    ,t.LineCost = s.LineCost
	    ,t.TaxAmount = s.TaxAmount
	    ,t.IsLTA = s.IsLTA
        ,t.dpt_auto_key = s.dpt_auto_key
	    ,t.BillingZipCode = s.BillingZipCode
	    ,t.BillingCountry = s.BillingCountry
	    ,t.ETLModifiedDate = GETDATE() 
WHEN NOT MATCHED THEN
INSERT (
      NativeId
	,NativeLineId
	,EmployeeId
	,CustomerId
	,PartId
	,SalesRepId
	,RouteId
	,DivisionId
	,InvoiceNumber
	,LineNumber
	,NativeSOLineId
	,InvoiceDate
	,InvoiceDateId
	,OrderDate
	,OrderDateId
	,ShipDate
	,PostDate
	,PostStatus
	,InvoiceType
	,ExchangeRate
	,QtyShipped
	,QtyBackOrdered
	,UnitCost
	,UnitPrice
	,LinePrice
	,LineCost
	,TaxAmount
	,IsLTA
    ,dpt_auto_key
	,BillingZipCode
	,BillingCountry
	,ETLImportDate
 
)
VALUES (
      s.NativeId
	,s.NativeLineId
	,s.EmployeeId
	,s.CustomerId
	,s.PartId
	,s.SalesRepId
	,s.RouteId
	,s.DivisionId
	,s.InvoiceNumber
	,s.LineNumber
	,s.NativeSOLineId
	,s.InvoiceDate
	,s.InvoiceDateId
	,s.OrderDate
	,s.OrderDateId
	,s.ShipDate
	,s.PostDate
	,s.PostStatus
	,s.InvoiceType
	,s.ExchangeRate
	,s.QtyShipped
	,s.QtyBackOrdered
	,s.UnitCost
	,s.UnitPrice
	,s.LinePrice
	,s.LineCost
	,s.TaxAmount
	,s.IsLTA
    ,s.dpt_auto_key
	,s.BillingZipCode
	,s.BillingCountry
	,s.ETLImportDate

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
