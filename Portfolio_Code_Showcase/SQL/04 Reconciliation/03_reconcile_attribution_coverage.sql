-- ==========================================================================
-- 03_reconcile_attribution_coverage.sql
-- --------------------------------------------------------------------------
-- What it does: Measures leases carrying no marketing trail.
-- What it demonstrates: Turns an absence into a measured KPI. Possible only
-- because canonical truth is held beside the CRM reconstruction.
-- Source: crm_pipeline.py (extracted verbatim)
-- ==========================================================================
MERGE pipeline.campaign_vendor_lookup AS target
        USING (VALUES (:cid, :vk, :ck, :cname, :vname, :ctype))
            AS source (CampaignId, VendorKey, ChannelKey,
                       CampaignName, VendorName, ChannelType)
        ON target.CampaignId = source.CampaignId
        WHEN MATCHED THEN UPDATE SET
            target.VendorKey    = source.VendorKey,
            target.ChannelKey   = source.ChannelKey,
            target.CampaignName = source.CampaignName,
            target.VendorName   = source.VendorName,
            target.ChannelType  = source.ChannelType,
            target.UpdatedAt    = SYSDATETIME()
        WHEN NOT MATCHED THEN INSERT
            (CampaignId, VendorKey, ChannelKey, CampaignName,
             VendorName, ChannelType, IsActive)
        VALUES
            (source.CampaignId, source.VendorKey, source.ChannelKey,
             source.CampaignName, source.VendorName, source.ChannelType, 1);
