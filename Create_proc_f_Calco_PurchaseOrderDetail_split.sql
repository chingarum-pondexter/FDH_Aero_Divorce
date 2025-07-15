SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*==========================================================================================================  
Author: Diksha  
Date: 4/10/2023  
Description: Merges new and updated records from Calco's purchase order detail data into the data   
 warehouse PurchaseOrder fact table.  
Notes: 08/23/2023 Partap Singh : Added Delete Statement Logic
  
==========================================================================================================*/
CREATE PROC [etl].[create_f_Calco_PurchaseOrderDetail_split]
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
                                    @ProcessName = 'etl.create_f_Calco_PurchaseOrderDetail_split',
                                    @SourceTableOrViewName = 'etl.v_f_calco_PurchaseOrderDetail_split',
                                    @TargetName = 'dwh.f_PurchaseOrderDetail',
                                    @logid = @logid OUTPUT;



        DECLARE @Upsert TABLE
        (
            Change VARCHAR(30)
        );
        SELECT @RowsRead = COUNT(1)
        FROM
        (SELECT VendorId FROM etl.v_f_calco_PurchaseOrderDetail_split) AS p;

        SELECT @RowsRead = COUNT(1)
        FROM etl.v_f_calco_PurchaseOrderDetail_split;
        IF
        (
            SELECT COUNT(*)FROM [etl].v_f_calco_PurchaseOrderDetail_split
        ) > (
        (
            SELECT COUNT(*)FROM dwh.f_PurchaseOrderDetail WHERE DivisionID = 12
        ) * .8
            )
        BEGIN
            DELETE dwh.f_PurchaseOrderDetail
            WHERE dpt_auto_key = 106;

            DECLARE @RunDate DATETIME = GETDATE();

            MERGE dwh.f_PurchaseOrderDetail t
            USING [etl].[v_f_calco_PurchaseOrderDetail_split] s
            ON t.VendorID = s.VendorId
               AND t.Partid = s.PartId
               AND t.PONumber = s.PONumber
               AND ISNULL(t.LineNumber, '') = ISNULL(s.LineNumber, '')
               AND t.DivisionID = s.DivisionId
               AND ISNULL(t.ReceivedDate, '1990-01-01') = ISNULL(s.ReceivedDate, '1990-01-01')
               AND t.UnitCost = s.UnitCost
            WHEN MATCHED THEN
                UPDATE SET t.[VendorID] = s.[VendorId],
                           t.[Partid] = s.[PartId],
                           t.[PONumber] = s.[PONumber],
                           t.[LineNumber] = s.[LineNumber],
                           t.[QuantityReceived] = s.[QuantityReceived],
                           t.[Complete] = s.[Complete],
                           t.[ReceivedDate] = s.[ReceivedDate],
                           t.[UnitCost] = s.[UnitCost],
                           t.[CalculatedValue] = s.[CalculatedValue],
                           t.ETLImportDate = GETDATE(),
                           t.dpt_auto_key = s.dpt_auto_key,
                           t.[DivisionID] = s.[DivisionId]
            WHEN NOT MATCHED THEN
                INSERT
                (
                    [VendorID],
                    [Partid],
                    [PONumber],
                    [LineNumber],
                    [QuantityReceived],
                    [Complete],
                    [ReceivedDate],
                    [UnitCost],
                    [CalculatedValue],
                    [ETLImportDate],
                    dpt_auto_key,
                    [DivisionID]
                )
                VALUES
                (s.[VendorId], s.[PartId], s.[PONumber], s.[LineNumber], s.[QuantityReceived], s.[Complete],
                 s.[ReceivedDate], s.[UnitCost], s.[CalculatedValue], s.[ETLImportDate], s.dpt_auto_key ,s.[DivisionId])
            OUTPUT $action
            INTO @Upsert;

            PRINT 'Success';
        END;
        ELSE
            PRINT 'Not enought staged records';
        --Adding Reconciliation data     
        SELECT @RowsWritten = ISNULL(SUM(   CASE
                                                WHEN Change = 'INSERT' THEN
                                                    1
                                                ELSE
                                                    0
                                            END
                                        ),
                                     0
                                    ),
               @RowsUpdated = ISNULL(SUM(   CASE
                                                WHEN Change = 'UPDATE' THEN
                                                    1
                                                ELSE
                                                    0
                                            END
                                        ),
                                     0
                                    )
        FROM @Upsert;


        SELECT @RowsByPassed = @RowsRead - @RowsUpdated - @RowsWritten;

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
