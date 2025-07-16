SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/*=====================================================
Author: Landon Ochs 
Date: 7/22/2020
Called By: SSIS - prc_CalcoQuantum
Purpose: Keeps the data warehouse tables up to date with the source

Notes:  2025-01-10 Wayne Chia: Added 3 fields to f_Shipment for ability to recreate Quantum report FDH 027
=====================================================*/

Create PROC [etl].[create_f_Calco_Shipment_split] 
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
                                    @ProcessName = 'etl.create_f_Calco_Shipment_split',  
                                    @SourceTableOrViewName = 'etl.v_f_Calco_Shipment_split',  
                                    @TargetName = 'dwh.f_Shipment',  
                                    @logid = @logid OUTPUT ;



		DECLARE @Upsert TABLE  
        (  
            Change VARCHAR(30)  
        );
        SELECT @RowsRead = COUNT(1)  
        FROM  
        (SELECT NativeId FROM etl.v_f_Calco_Shipment_split) AS p;  

WITH CTE_Shipment AS (
    SELECT 
        NativeId,
        DivisionId,
        CustomerId,
        CurrencyId,
        [EmployeeId],
        NativeSOId,
        NativeInvoiceId,
        NativeSvcAutoKey,
        ShipmentNumber,
        TrackingNumber,
        IsOpen,
        ShipDate,
        ShipDateId,
        EntryDate,
        EntryDateId,
        InvoiceDate,
        InvoicePrice,
        InvoiceNumber,
        PackerName,
        WarehouseLocation,
        ShipmentPriority,
        NativeModifiedDate,
        NativeCreatedDate,
        ETLImportDate,
        [NativeSODId],
		SMS_AUTO_KEY,
		SM_UDF_002,
		WHS_TO,
        dpt_auto_key,
        ROW_NUMBER() OVER (PARTITION BY NativeId, DivisionId ORDER BY NativeModifiedDate DESC) AS RowNum
    FROM [etl].v_f_Calco_Shipment_split
)
MERGE dwh.f_Shipment AS t
USING (
    SELECT *
    FROM CTE_Shipment
    WHERE RowNum = 1 -- Keep only the latest row per NativeId and DivisionId
) AS s
ON t.NativeId = s.NativeId
   AND t.DivisionId = s.DivisionId
WHEN MATCHED THEN
    UPDATE SET 
        t.CustomerId = s.CustomerId,
        t.CurrencyId = s.CurrencyId,
        t.[EmployeeId] = s.[EmployeeId],
        t.NativeSOId = s.NativeSOId,
        t.NativeInvoiceId = s.NativeInvoiceId,
        t.NativeSvcAutoKey = s.NativeSvcAutoKey,
        t.ShipmentNumber = s.ShipmentNumber,
        t.TrackingNumber = s.TrackingNumber,
        t.IsOpen = s.IsOpen,
        t.ShipDate = s.ShipDate,
        t.ShipDateId = s.ShipDateId,
        t.EntryDate = s.EntryDate,
        t.EntryDateId = s.EntryDateId,
        t.InvoiceDate = s.InvoiceDate,
        t.InvoicePrice = s.InvoicePrice,
        t.InvoiceNumber = s.InvoiceNumber,
        t.PackerName = s.PackerName,
        t.WarehouseLocation = s.WarehouseLocation,
        t.ShipmentPriority = s.ShipmentPriority,
        t.NativeModifiedDate = s.NativeModifiedDate,
        t.NativeCreatedDate = s.NativeCreatedDate,
        t.ETLModifiedDate = GETDATE(),
        t.[NativeSODId] = s.[NativeSODId],
		t.SMS_AUTO_KEY	= s.SMS_AUTO_KEY,
		t.SM_UDF_002	= s.SM_UDF_002,
		t.WHS_TO		= s.WHS_TO,
        t.dpt_auto_key = s.dpt_auto_key
WHEN NOT MATCHED THEN
    INSERT (
        NativeId,
        CustomerId,
        CurrencyId,
        [EmployeeId],
        DivisionId,
        NativeSOId,
        NativeInvoiceId,
        NativeSvcAutoKey,
        ShipmentNumber,
        TrackingNumber,
        IsOpen,
        ShipDate,
        ShipDateId,
        EntryDate,
        EntryDateId,
        InvoiceDate,
        InvoicePrice,
        InvoiceNumber,
        PackerName,
        WarehouseLocation,
        ShipmentPriority,
        NativeModifiedDate,
        NativeCreatedDate,
        ETLImportDate,
        [NativeSODId],
		SMS_AUTO_KEY,
		SM_UDF_002,
		WHS_TO,
        dpt_auto_key
    )
    VALUES (
        s.NativeId,
        s.CustomerId,
        s.CurrencyId,
        s.[EmployeeId],
        s.DivisionId,
        s.NativeSOId,
        s.NativeInvoiceId,
        s.NativeSvcAutoKey,
        s.ShipmentNumber,
        s.TrackingNumber,
        s.IsOpen,
        s.ShipDate,
        s.ShipDateId,
        s.EntryDate,
        s.EntryDateId,
        s.InvoiceDate,
        s.InvoicePrice,
        s.InvoiceNumber,
        s.PackerName,
        s.WarehouseLocation,
        s.ShipmentPriority,
        s.NativeModifiedDate,
        s.NativeCreatedDate,
        s.ETLImportDate,
        s.[NativeSODId],
		s.SMS_AUTO_KEY,
		s.SM_UDF_002,
		s.WHS_TO,
        s.dpt_auto_key
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
