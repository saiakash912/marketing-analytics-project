
/*
Senior Marketing Analyst – Technical Assignment (SQL Server Implementation)
Date: 2026-04-10

This script creates a clean, scalable model in SQL Server to unify multi-channel ad data
(Facebook, Google Ads, TikTok) and prepare a semantic layer for BI.

Sections
1) Safety: idempotent schemas
2) RAW layer: source tables (1:1 with CSV columns)
3) LOAD helpers: BULK INSERT templates (local) + OPENROWSET(BULK...) templates (Azure SQL)
4) DWH layer: conformed fact table (typed, conformed column names)
5) UPSERT from RAW -> DWH (aggregating duplicates per [platform,date,campaign_id,adgroup_id])
6) MART views: KPI view with derived metrics
7) Performance: indexes
*/

/* =============================
 1) SAFETY & SCHEMAS
============================= */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'raw') EXEC('CREATE SCHEMA raw');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dwh') EXEC('CREATE SCHEMA dwh');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mart') EXEC('CREATE SCHEMA mart');
GO



/* =============================
 2) RAW LAYER – SOURCE TABLES
   (Columns mirror CSVs exactly; all nullable)
============================= */
IF OBJECT_ID('raw.facebook_ads') IS NOT NULL DROP TABLE raw.facebook_ads;
CREATE TABLE raw.facebook_ads (
    [date]              date        NULL,
    [campaign_id]       varchar(50) NULL,
    [campaign_name]     nvarchar(200) NULL,
    [ad_set_id]         varchar(50) NULL,
    [ad_set_name]       nvarchar(200) NULL,
    [impressions]       bigint NULL,
    [clicks]            bigint NULL,
    [spend]             decimal(19,4) NULL,
    [conversions]       bigint NULL,
    [video_views]       bigint NULL,
    [engagement_rate]   decimal(19,6) NULL,
    [reach               ] bigint NULL,
    [frequency]         decimal(19,6) NULL
);

IF OBJECT_ID('raw.google_ads') IS NOT NULL DROP TABLE raw.google_ads;
CREATE TABLE raw.google_ads (
    [date]                      date        NULL,
    [campaign_id]               varchar(50) NULL,
    [campaign_name]             nvarchar(200) NULL,
    [ad_group_id]               varchar(50) NULL,
    [ad_group_name]             nvarchar(200) NULL,
    [impressions]               bigint NULL,
    [clicks]                    bigint NULL,
    [cost]                      decimal(19,4) NULL,
    [conversions]               bigint NULL,
    [conversion_value]          decimal(19,4) NULL,
    [ctr]                       decimal(19,6) NULL,
    [avg_cpc]                   decimal(19,6) NULL,
    [quality_score]             int NULL,
    [search_impression_share]   decimal(19,6) NULL
);

IF OBJECT_ID('raw.tiktok_ads') IS NOT NULL DROP TABLE raw.tiktok_ads;
CREATE TABLE raw.tiktok_ads (
    [date]              date        NULL,
    [campaign_id]       varchar(50) NULL,
    [campaign_name]     nvarchar(200) NULL,
    [adgroup_id]        varchar(50) NULL,
    [adgroup_name]      nvarchar(200) NULL,
    [impressions]       bigint NULL,
    [clicks]            bigint NULL,
    [cost]              decimal(19,4) NULL,
    [conversions]       bigint NULL,
    [video_views]       bigint NULL,
    [video_watch_25]    bigint NULL,
    [video_watch_50]    bigint NULL,
    [video_watch_75]    bigint NULL,
    [video_watch_100]   bigint NULL,
    [likes]             bigint NULL,
    [shares]            bigint NULL,
    [comments]          bigint NULL
);
GO


/* =============================
 2) STAGGING LAYER – SOURCE TABLES
   (Columns mirror CSVs exactly; all nullable)
============================= */
IF OBJECT_ID('raw.facebook_ads') IS NOT NULL DROP TABLE raw.facebook_ads;
CREATE TABLE stg.facebook_ads (
    [date]              date        NULL,
    [campaign_id]       varchar(50) NULL,
    [campaign_name]     nvarchar(200) NULL,
    [ad_set_id]         varchar(50) NULL,
    [ad_set_name]       nvarchar(200) NULL,
    [impressions]       bigint NULL,
    [clicks]            bigint NULL,
    [spend]             decimal(19,4) NULL,
    [conversions]       bigint NULL,
    [video_views]       bigint NULL,
    [engagement_rate]   decimal(19,6) NULL,
    [reach               ] bigint NULL,
    [frequency]         decimal(19,6) NULL,
    [source_file]       nvarchar(260) NULL,
    [load_dt]           datetime2(0) NOT NULL CONSTRAINT DF_raw_fb_load_dt DEFAULT (sysdatetime())
);

IF OBJECT_ID('raw.google_ads') IS NOT NULL DROP TABLE raw.google_ads;
CREATE TABLE stg.google_ads (
    [date]                      date        NULL,
    [campaign_id]               varchar(50) NULL,
    [campaign_name]             nvarchar(200) NULL,
    [ad_group_id]               varchar(50) NULL,
    [ad_group_name]             nvarchar(200) NULL,
    [impressions]               bigint NULL,
    [clicks]                    bigint NULL,
    [cost]                      decimal(19,4) NULL,
    [conversions]               bigint NULL,
    [conversion_value]          decimal(19,4) NULL,
    [ctr]                       decimal(19,6) NULL,
    [avg_cpc]                   decimal(19,6) NULL,
    [quality_score]             int NULL,
    [search_impression_share]   decimal(19,6) NULL,
    [source_file]               nvarchar(260) NULL,
    [load_dt]                   datetime2(0) NOT NULL CONSTRAINT DF_raw_ga_load_dt DEFAULT (sysdatetime())
);

IF OBJECT_ID('raw.tiktok_ads') IS NOT NULL DROP TABLE raw.tiktok_ads;
CREATE TABLE stg.tiktok_ads (
    [date]              date        NULL,
    [campaign_id]       varchar(50) NULL,
    [campaign_name]     nvarchar(200) NULL,
    [adgroup_id]        varchar(50) NULL,
    [adgroup_name]      nvarchar(200) NULL,
    [impressions]       bigint NULL,
    [clicks]            bigint NULL,
    [cost]              decimal(19,4) NULL,
    [conversions]       bigint NULL,
    [video_views]       bigint NULL,
    [video_watch_25]    bigint NULL,
    [video_watch_50]    bigint NULL,
    [video_watch_75]    bigint NULL,
    [video_watch_100]   bigint NULL,
    [likes]             bigint NULL,
    [shares]            bigint NULL,
    [comments]          bigint NULL,
    [source_file]       nvarchar(260) NULL,
    [load_dt]           datetime2(0) NOT NULL CONSTRAINT DF_raw_tt_load_dt DEFAULT (sysdatetime())
);
GO

/* =============================
 3) LOAD – BULK INSERT / OPENROWSET templates
   -> Replace <PATH> or STORAGE CREDENTIAL details before running
============================= */
BULK INSERT raw.facebook_ads
FROM 'C:\Downloads\Assignment\01_facebook_ads.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',  -- try 0x0a if needed
    CODEPAGE = '65001',
    TABLOCK
);

BULK INSERT raw.google_ads
FROM 'C:\Downloads\Assignment\02_google_ads.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',  -- try 0x0a if needed
    CODEPAGE = '65001',
    TABLOCK
);

BULK INSERT raw.tiktok_ads
FROM '\Downloads\Assignment\03_tiktok_ads.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',  -- try 0x0a if needed
    CODEPAGE = '65001',
    TABLOCK
);


/* =============================
 DWH – INSERTION INTO STAGGING
============================= */

INSERT INTO stg.facebook_ads
select     [date],
    [campaign_id],
    [campaign_name],
    [ad_set_id],
    [ad_set_name],
    [impressions],
    [clicks],
    [spend],
    [conversions],
    [video_views],
    [engagement_rate],
    [reach],
    [frequency],
    'facebook' + CONVERT(VARCHAR(8), GETDATE(), 112) + '.csv',,
     GETDATE()   
from raw.facebook_ads


INSERT INTO stg.google_ads
select     [date],
    [campaign_id],    
    [campaign_name],  
    [ad_group_id],  
    [ad_group_name],  
    [impressions], 
    [clicks],         
    [cost],           
    [conversions],    
    [conversion_value],
    [ctr],              
    [avg_cpc],          
    [quality_score],    
    [search_impression_share], 
    'google' + CONVERT(VARCHAR(8), GETDATE(), 112) + '.csv',
     GETDATE()   
from raw.google_ads


INSERT INTO stg.tiktok_ads
select     
	[date],          
	[campaign_id],    
	[campaign_name],  
	[adgroup_id],     
	[adgroup_name],   
	[impressions],    
	[clicks],         
	[cost],           
	[conversions],    
	[video_views],    
	[video_watch_25], 
	[video_watch_50], 
	[video_watch_75], 
	[video_watch_100],
	[likes],          
	[shares],         
	[comments] ,      
    'tiktok' + CONVERT(VARCHAR(8), GETDATE(), 112) + '.csv',,
     GETDATE()   
from raw.tiktok_ads



/* =============================
 4) DWH – CONFORMED FACT TABLE
============================= */
IF OBJECT_ID('dwh.fact_ad_performance') IS NOT NULL DROP TABLE dwh.fact_ad_performance;
CREATE TABLE dwh.fact_ad_performance (
    [date]                      date            NOT NULL,
    [platform]                  varchar(20)     NOT NULL, -- 'facebook'|'google'|'tiktok'
    [campaign_id]               varchar(50)     NOT NULL,
    [campaign_name]             nvarchar(200)   NULL,
    [adgroup_id]                varchar(50)     NOT NULL,
    [adgroup_name]              nvarchar(200)   NULL,
    [impressions]               bigint          NULL,
    [clicks]                    bigint          NULL,
    [cost]                      decimal(19,4)   NULL,
    [conversions]               bigint          NULL,
    [conversion_value]          decimal(19,4)   NULL,
    [video_views]               bigint          NULL,
    [reach]                     bigint          NULL,
    [frequency]                 decimal(19,6)   NULL,
    [engagement_rate]           decimal(19,6)   NULL,
    [quality_score]             int             NULL,
    [search_impression_share]   decimal(19,6)   NULL,
    [likes]                     bigint          NULL,
    [shares]                    bigint          NULL,
    [comments]                  bigint          NULL,
    [source_file]               nvarchar(260)   NULL,
    [load_dt]                   datetime2(0)    NOT NULL CONSTRAINT DF_dwh_fact_load_dt DEFAULT (sysdatetime()),
    CONSTRAINT PK_fact_ad_performance PRIMARY KEY CLUSTERED ([date], [platform], [campaign_id], [adgroup_id])
);
GO

/* =============================
 5) UPSERT / BUILD FACT FROM RAW (aggregate duplicates by grain)
============================= */
-- Facebook -> Fact
WITH fb AS (
    SELECT
        [date], 'facebook' AS platform,
        campaign_id, campaign_name, ad_set_id AS adgroup_id, ad_set_name AS adgroup_name,
        SUM(impressions) impressions,
        SUM(clicks) clicks,
        SUM(spend) cost,
        SUM(conversions) conversions,
        CAST(NULL AS decimal(19,4)) AS conversion_value,
        SUM(video_views) AS video_views,
        SUM(reach) AS reach,
        AVG(frequency) AS frequency,
        AVG(engagement_rate) AS engagement_rate,
        CAST(NULL AS int) AS quality_score,
        CAST(NULL AS decimal(19,6)) AS search_impression_share,
        CAST(NULL AS bigint) AS likes,
        CAST(NULL AS bigint) AS shares,
        CAST(NULL AS bigint) AS comments,
        MAX(source_file) AS source_file,
        MAX(load_dt) AS load_dt
    FROM stg.facebook_ads
    GROUP BY [date], campaign_id, campaign_name, ad_set_id, ad_set_name
),
-- Google -> Fact
ga AS (
    SELECT
        [date], 'google' AS platform,
        campaign_id, campaign_name, ad_group_id AS adgroup_id, ad_group_name AS adgroup_name,
        SUM(impressions) impressions,
        SUM(clicks) clicks,
        SUM(cost) cost,
        SUM(conversions) conversions,
        SUM(conversion_value) conversion_value,
        CAST(NULL AS bigint) AS video_views,
        CAST(NULL AS bigint) AS reach,
        CAST(NULL AS decimal(19,6)) AS frequency,
        CAST(NULL AS decimal(19,6)) AS engagement_rate,
        CAST(AVG(quality_score) AS int) AS quality_score,
        AVG(search_impression_share) AS search_impression_share,
        CAST(NULL AS bigint) AS likes,
        CAST(NULL AS bigint) AS shares,
        CAST(NULL AS bigint) AS comments,
        MAX(source_file) AS source_file,
        MAX(load_dt) AS load_dt
    FROM stg.google_ads
    GROUP BY [date], campaign_id, campaign_name, ad_group_id, ad_group_name
),
-- TikTok -> Fact
tt AS (
    SELECT
        [date], 'tiktok' AS platform,
        campaign_id, campaign_name, adgroup_id, adgroup_name,
        SUM(impressions) impressions,
        SUM(clicks) clicks,
        SUM(cost) cost,
        SUM(conversions) conversions,
        CAST(NULL AS decimal(19,4)) AS conversion_value,
        SUM(video_views) AS video_views,
        CAST(NULL AS bigint) AS reach,
        CAST(NULL AS decimal(19,6)) AS frequency,
        CAST(NULL AS decimal(19,6)) AS engagement_rate,
        CAST(NULL AS int) AS quality_score,
        CAST(NULL AS decimal(19,6)) AS search_impression_share,
        SUM(likes) AS likes,
        SUM(shares) AS shares,
        SUM(comments) AS comments,
        MAX(source_file) AS source_file,
        MAX(load_dt) AS load_dt
    FROM stg.tiktok_ads
    GROUP BY [date], campaign_id, campaign_name, adgroup_id, adgroup_name
)
INSERT INTO dwh.fact_ad_performance (
    [date],[platform],[campaign_id],[campaign_name],[adgroup_id],[adgroup_name],
    impressions, clicks, cost, conversions, conversion_value, video_views, reach, frequency,
    engagement_rate, quality_score, search_impression_share, likes, shares, comments,
    source_file, load_dt)
SELECT * FROM fb
UNION ALL
SELECT * FROM ga
UNION ALL
SELECT * FROM tt;
GO

/* =============================
 6) MART – ANALYTICAL VIEW WITH DERIVED KPIs
============================= */
IF OBJECT_ID('mart.v_ad_kpis') IS NOT NULL DROP VIEW mart.v_ad_kpis;
GO
CREATE VIEW mart.v_ad_kpis AS
SELECT
    f.[date], f.[platform], f.[campaign_id], f.[campaign_name], f.[adgroup_id], f.[adgroup_name],
    f.impressions, f.clicks, f.cost, f.conversions, f.conversion_value, f.video_views, f.reach, f.frequency,
    f.engagement_rate, f.quality_score, f.search_impression_share, f.likes, f.shares, f.comments,
    -- Derived KPIs (null-safe)
    CAST(1.0 * f.clicks / NULLIF(f.impressions,0) AS decimal(19,6)) AS ctr,
    CAST(f.cost / NULLIF(f.clicks,0) AS decimal(19,6)) AS cpc,
    CAST(f.cost * 1000.0 / NULLIF(f.impressions,0) AS decimal(19,6)) AS cpm,
    CAST(f.cost / NULLIF(f.conversions,0) AS decimal(19,6)) AS cpa,
    CAST(f.conversion_value / NULLIF(f.cost,0) AS decimal(19,6)) AS roas
FROM dwh.fact_ad_performance f;
GO

/* =============================
 7) PERFORMANCE
============================= */
-- Columnstore for fast scans
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes WHERE name = 'CCI_fact_ad_performance' AND object_id = OBJECT_ID('dwh.fact_ad_performance')
)
BEGIN
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_ad_performance ON dwh.fact_ad_performance;
END
GO

-- Helpful narrow index for date/platform filters
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes WHERE name = 'IX_fact_date_platform' AND object_id = OBJECT_ID('dwh.fact_ad_performance')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_fact_date_platform ON dwh.fact_ad_performance([date],[platform]) INCLUDE (impressions, clicks, cost, conversions, conversion_value);
END
GO
