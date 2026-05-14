-- ============================================================
-- CHICAGO CRIME DATA LAKE DATABRICKS SQL
-- Script: 00_create_databases_and_schemas.sql
-- Purpose: One-time setup of all databases (zones) and
--          the metadata/control schema
-- Run: Once, before any other script
-- ============================================================

-- Set the catalog context for Unity Catalog
USE CATALOG chicago_crimes_workspace;

-- ── Zone databases ──────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS bronze
COMMENT 'Raw, immutable ingestion zone — files loaded as-is, all columns VARCHAR';

CREATE DATABASE IF NOT EXISTS silver
COMMENT 'Cleaned, typed, deduplicated, conformed zone with quality gates';

CREATE DATABASE IF NOT EXISTS gold
COMMENT 'Analytics-ready curated tables, dimensions, facts, and views';

-- ── Metadata/control database ───────────────────────────────
CREATE DATABASE IF NOT EXISTS meta
COMMENT 'ETL run logs, ingestion logs, quality gate failure logs, lineage';

-- ── Confirm ─────────────────────────────────────────────────
SHOW DATABASES;