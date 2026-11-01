-- Databricks notebook source
-- CareFlow360 bootstrap SQL.
-- Replace <CATALOG> with the output of SELECT current_catalog().

SELECT current_catalog() AS current_catalog;

USE CATALOG `<CATALOG>`;

CREATE SCHEMA IF NOT EXISTS careflow360_dev
COMMENT 'CareFlow360 synthetic healthcare lakehouse';

CREATE VOLUME IF NOT EXISTS careflow360_dev.landing
COMMENT 'Landing area for synthetic/public CareFlow360 events';

SHOW SCHEMAS IN `<CATALOG>`;
SHOW VOLUMES IN `<CATALOG>`.careflow360_dev;
