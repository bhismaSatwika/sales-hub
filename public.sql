/*
 Navicat Premium Dump SQL

 Source Server         : L21
 Source Server Type    : PostgreSQL
 Source Server Version : 100023 (100023)
 Source Host           : 170.1.20.21:5432
 Source Catalog        : distributionship_prod_v2
 Source Schema         : public

 Target Server Type    : PostgreSQL
 Target Server Version : 100023 (100023)
 File Encoding         : 65001

 Date: 01/04/2026 17:06:58
*/


-- ----------------------------
-- Sequence structure for files_retur_id_file_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."files_retur_id_file_seq";
CREATE SEQUENCE "public"."files_retur_id_file_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for files_upload_id_file_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."files_upload_id_file_seq";
CREATE SEQUENCE "public"."files_upload_id_file_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_approval_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_approval_id_seq";
CREATE SEQUENCE "public"."master_approval_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_approval_type_id__seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_approval_type_id__seq";
CREATE SEQUENCE "public"."master_approval_type_id__seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_approval_type_id_type_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_approval_type_id_type_seq";
CREATE SEQUENCE "public"."master_approval_type_id_type_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_biaya_admin_id_biaya_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_biaya_admin_id_biaya_seq";
CREATE SEQUENCE "public"."master_biaya_admin_id_biaya_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_cabang_id_cabang_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_cabang_id_cabang_seq";
CREATE SEQUENCE "public"."master_cabang_id_cabang_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_company_id_company_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_company_id_company_seq";
CREATE SEQUENCE "public"."master_company_id_company_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_customer_id_customer_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_customer_id_customer_seq";
CREATE SEQUENCE "public"."master_customer_id_customer_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_customer_id_increment_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_customer_id_increment_seq";
CREATE SEQUENCE "public"."master_customer_id_increment_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_jenis_pembayaran_id_pembayaran_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_jenis_pembayaran_id_pembayaran_seq";
CREATE SEQUENCE "public"."master_jenis_pembayaran_id_pembayaran_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_kategori_id_kategori_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_kategori_id_kategori_seq";
CREATE SEQUENCE "public"."master_kategori_id_kategori_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_minimum_qty_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_minimum_qty_id_seq";
CREATE SEQUENCE "public"."master_minimum_qty_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_pajak_id_pajak_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_pajak_id_pajak_seq";
CREATE SEQUENCE "public"."master_pajak_id_pajak_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_produk_id_produk_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_produk_id_produk_seq";
CREATE SEQUENCE "public"."master_produk_id_produk_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_sales_price_id_price_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_sales_price_id_price_seq";
CREATE SEQUENCE "public"."master_sales_price_id_price_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_template_download_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_template_download_id_seq";
CREATE SEQUENCE "public"."master_template_download_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_uom_satuan_id_uom_satuan_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_uom_satuan_id_uom_satuan_seq";
CREATE SEQUENCE "public"."master_uom_satuan_id_uom_satuan_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_user_id_user_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_user_id_user_seq";
CREATE SEQUENCE "public"."master_user_id_user_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for master_user_role_id_role_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."master_user_role_id_role_seq";
CREATE SEQUENCE "public"."master_user_role_id_role_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_approval_detail_detail_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_approval_detail_detail_id_seq";
CREATE SEQUENCE "public"."trans_approval_detail_detail_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_harga_produk_id_trans_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_harga_produk_id_trans_seq";
CREATE SEQUENCE "public"."trans_harga_produk_id_trans_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_detail_bad_stock_header_id_increment_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_detail_bad_stock_header_id_increment_seq";
CREATE SEQUENCE "public"."trans_inventory_detail_bad_stock_header_id_increment_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_detail_bad_stock_id_increment_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_detail_bad_stock_id_increment_seq";
CREATE SEQUENCE "public"."trans_inventory_detail_bad_stock_id_increment_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_detail_id_trans_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_detail_id_trans_seq";
CREATE SEQUENCE "public"."trans_inventory_detail_id_trans_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_detail_mutasi_id_trans_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_detail_mutasi_id_trans_seq";
CREATE SEQUENCE "public"."trans_inventory_detail_mutasi_id_trans_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_holding_delivery_preparation_header_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_holding_delivery_preparation_header_id_seq";
CREATE SEQUENCE "public"."trans_inventory_holding_delivery_preparation_header_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_holding_delivery_preparation_id_increment_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_holding_delivery_preparation_id_increment_seq";
CREATE SEQUENCE "public"."trans_inventory_holding_delivery_preparation_id_increment_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_holding_delivery_preparation_id_trans_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_holding_delivery_preparation_id_trans_seq";
CREATE SEQUENCE "public"."trans_inventory_holding_delivery_preparation_id_trans_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_holding_submit_id_increment_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_holding_submit_id_increment_seq";
CREATE SEQUENCE "public"."trans_inventory_holding_submit_id_increment_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_holding_submit_id_trans_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_holding_submit_id_trans_seq";
CREATE SEQUENCE "public"."trans_inventory_holding_submit_id_trans_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_holding_transfer_id_incement_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_holding_transfer_id_incement_seq";
CREATE SEQUENCE "public"."trans_inventory_holding_transfer_id_incement_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_holding_transfer_id_trans_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_holding_transfer_id_trans_seq";
CREATE SEQUENCE "public"."trans_inventory_holding_transfer_id_trans_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_subsidiary_delivery_order_id_increment_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_subsidiary_delivery_order_id_increment_seq";
CREATE SEQUENCE "public"."trans_inventory_subsidiary_delivery_order_id_increment_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_subsidiary_delivery_order_id_trans_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_subsidiary_delivery_order_id_trans_seq";
CREATE SEQUENCE "public"."trans_inventory_subsidiary_delivery_order_id_trans_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_subsidiary_invoice_id_increment_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_subsidiary_invoice_id_increment_seq";
CREATE SEQUENCE "public"."trans_inventory_subsidiary_invoice_id_increment_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_subsidiary_invoice_id_trans_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_subsidiary_invoice_id_trans_seq";
CREATE SEQUENCE "public"."trans_inventory_subsidiary_invoice_id_trans_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_subsidiary_receipt_id_trans_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_subsidiary_receipt_id_trans_seq";
CREATE SEQUENCE "public"."trans_inventory_subsidiary_receipt_id_trans_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_subsidiary_receipt_transfer_id_increment_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_subsidiary_receipt_transfer_id_increment_seq";
CREATE SEQUENCE "public"."trans_inventory_subsidiary_receipt_transfer_id_increment_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_subsidiary_retur_detail_id_detail_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_subsidiary_retur_detail_id_detail_seq";
CREATE SEQUENCE "public"."trans_inventory_subsidiary_retur_detail_id_detail_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_subsidiary_retur_header_id_increment_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_subsidiary_retur_header_id_increment_seq";
CREATE SEQUENCE "public"."trans_inventory_subsidiary_retur_header_id_increment_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_subsidiary_sales_order_header_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_subsidiary_sales_order_header_id_seq";
CREATE SEQUENCE "public"."trans_inventory_subsidiary_sales_order_header_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_subsidiary_sales_order_id_increment_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_subsidiary_sales_order_id_increment_seq";
CREATE SEQUENCE "public"."trans_inventory_subsidiary_sales_order_id_increment_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_inventory_subsidiary_sales_order_id_trans_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_inventory_subsidiary_sales_order_id_trans_seq";
CREATE SEQUENCE "public"."trans_inventory_subsidiary_sales_order_id_trans_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_produk_harga_jual_id_increment_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_produk_harga_jual_id_increment_seq";
CREATE SEQUENCE "public"."trans_produk_harga_jual_id_increment_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_sales_order_paid_payment_id_trans_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_sales_order_paid_payment_id_trans_seq";
CREATE SEQUENCE "public"."trans_sales_order_paid_payment_id_trans_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_sales_order_paid_payment_inv_id__seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_sales_order_paid_payment_inv_id__seq";
CREATE SEQUENCE "public"."trans_sales_order_paid_payment_inv_id__seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_sales_recap_detail_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_sales_recap_detail_id_seq";
CREATE SEQUENCE "public"."trans_sales_recap_detail_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Sequence structure for trans_sales_recap_header_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "public"."trans_sales_recap_header_id_seq";
CREATE SEQUENCE "public"."trans_sales_recap_header_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;

-- ----------------------------
-- Table structure for data_pasar_pantuan
-- ----------------------------
DROP TABLE IF EXISTS "public"."data_pasar_pantuan";
CREATE TABLE "public"."data_pasar_pantuan" (
  "id_prov" int4,
  "provinsi" varchar(255) COLLATE "pg_catalog"."default",
  "target" int4,
  "real" int4,
  "company_id" int4
)
;

-- ----------------------------
-- Table structure for files_retur
-- ----------------------------
DROP TABLE IF EXISTS "public"."files_retur";
CREATE TABLE "public"."files_retur" (
  "id_file" int4 NOT NULL DEFAULT nextval('files_retur_id_file_seq'::regclass),
  "id_header" varchar(255) COLLATE "pg_catalog"."default",
  "id_detail" int8,
  "file_name" varchar(255) COLLATE "pg_catalog"."default",
  "files" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now()
)
;

-- ----------------------------
-- Table structure for files_upload
-- ----------------------------
DROP TABLE IF EXISTS "public"."files_upload";
CREATE TABLE "public"."files_upload" (
  "id_file" int4 NOT NULL DEFAULT nextval('files_upload_id_file_seq'::regclass),
  "id_trans" varchar(255) COLLATE "pg_catalog"."default",
  "file_name" varchar(255) COLLATE "pg_catalog"."default",
  "files" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now()
)
;

-- ----------------------------
-- Table structure for master_approval
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_approval";
CREATE TABLE "public"."master_approval" (
  "id" int4 NOT NULL DEFAULT nextval('master_approval_id_seq'::regclass),
  "username" varchar(255) COLLATE "pg_catalog"."default",
  "approval_order" int4,
  "active" bool,
  "release" bool,
  "updateindb" timestamp(6) DEFAULT now(),
  "issued_by" varchar(255) COLLATE "pg_catalog"."default",
  "id_type" int8
)
;

-- ----------------------------
-- Table structure for master_approval_status
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_approval_status";
CREATE TABLE "public"."master_approval_status" (
  "id_status" int4 NOT NULL DEFAULT nextval('master_approval_type_id_type_seq'::regclass),
  "status_name" varchar(255) COLLATE "pg_catalog"."default",
  "release" bool,
  "active" bool
)
;

-- ----------------------------
-- Table structure for master_approval_type
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_approval_type";
CREATE TABLE "public"."master_approval_type" (
  "id_" int8 NOT NULL DEFAULT nextval('master_approval_type_id__seq'::regclass),
  "id_type" int8,
  "nama_approval" varchar(255) COLLATE "pg_catalog"."default",
  "active" bool,
  "release" bool,
  "updateindb" timestamp(6) DEFAULT now()
)
;

-- ----------------------------
-- Table structure for master_biaya_admin
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_biaya_admin";
CREATE TABLE "public"."master_biaya_admin" (
  "id_biaya" int4 NOT NULL DEFAULT nextval('master_biaya_admin_id_biaya_seq'::regclass),
  "id_company" int4,
  "id_cabang" int4,
  "biaya" numeric(32,2),
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for master_company
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_company";
CREATE TABLE "public"."master_company" (
  "id_company" int4 NOT NULL DEFAULT nextval('master_company_id_company_seq'::regclass),
  "company_name" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "kode" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false,
  "id_company_va" int8
)
;

-- ----------------------------
-- Table structure for master_company_cabang
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_company_cabang";
CREATE TABLE "public"."master_company_cabang" (
  "id_cabang" int4 NOT NULL DEFAULT nextval('master_cabang_id_cabang_seq'::regclass),
  "id_company" int4,
  "cabang_name" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false,
  "is_pusat" bool DEFAULT false,
  "latitude" varchar(255) COLLATE "pg_catalog"."default",
  "longitude" varchar(255) COLLATE "pg_catalog"."default",
  "unit_geografis" varchar(255) COLLATE "pg_catalog"."default",
  "provinsi_id" int4
)
;

-- ----------------------------
-- Table structure for master_customer
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_customer";
CREATE TABLE "public"."master_customer" (
  "id_customer" varchar(64) COLLATE "pg_catalog"."default" NOT NULL DEFAULT nextval('master_customer_id_customer_seq'::regclass),
  "nama_customer" varchar(255) COLLATE "pg_catalog"."default",
  "cabang_id" int4,
  "company_id" int4,
  "alamat" text COLLATE "pg_catalog"."default",
  "npwp" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false,
  "no_ktp" varchar(255) COLLATE "pg_catalog"."default",
  "no_hp" varchar(255) COLLATE "pg_catalog"."default",
  "email" varchar(255) COLLATE "pg_catalog"."default",
  "kode_prov" int8,
  "kode_kotakab" varchar(255) COLLATE "pg_catalog"."default",
  "kode_kec" varchar(255) COLLATE "pg_catalog"."default",
  "kode_kel" varchar(255) COLLATE "pg_catalog"."default",
  "account_va" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "account_bank_name" varchar(255) COLLATE "pg_catalog"."default",
  "id_customer_lama" varchar(64) COLLATE "pg_catalog"."default",
  "id_customer_baru" varchar(64) COLLATE "pg_catalog"."default",
  "id_increment" int4 NOT NULL DEFAULT nextval('master_customer_id_increment_seq'::regclass),
  "nama_btn" varchar(255) COLLATE "pg_catalog"."default",
  "is_pph" bool DEFAULT false
)
;

-- ----------------------------
-- Table structure for master_customer_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_customer_copy1";
CREATE TABLE "public"."master_customer_copy1" (
  "id_customer" int8 NOT NULL DEFAULT nextval('master_customer_id_customer_seq'::regclass),
  "nama_customer" varchar(255) COLLATE "pg_catalog"."default",
  "cabang_id" int4,
  "company_id" int4,
  "alamat" text COLLATE "pg_catalog"."default",
  "npwp" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false,
  "no_ktp" varchar(255) COLLATE "pg_catalog"."default",
  "no_hp" varchar(255) COLLATE "pg_catalog"."default",
  "email" varchar(255) COLLATE "pg_catalog"."default",
  "kode_prov" varchar(255) COLLATE "pg_catalog"."default",
  "kode_kotakab" varchar(255) COLLATE "pg_catalog"."default",
  "kode_kec" varchar(255) COLLATE "pg_catalog"."default",
  "kode_kel" varchar(255) COLLATE "pg_catalog"."default",
  "account_va" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "account_bank_name" varchar(255) COLLATE "pg_catalog"."default",
  "id_customer_lama" int4,
  "id_customer_baru" int8 NOT NULL,
  "id_increment" int4 NOT NULL DEFAULT nextval('master_customer_id_increment_seq'::regclass),
  "nama_btn" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for master_customer_copy2
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_customer_copy2";
CREATE TABLE "public"."master_customer_copy2" (
  "id_customer" varchar(64) COLLATE "pg_catalog"."default" NOT NULL DEFAULT nextval('master_customer_id_customer_seq'::regclass),
  "nama_customer" varchar(255) COLLATE "pg_catalog"."default",
  "cabang_id" int4,
  "company_id" int4,
  "alamat" text COLLATE "pg_catalog"."default",
  "npwp" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false,
  "no_ktp" varchar(255) COLLATE "pg_catalog"."default",
  "no_hp" varchar(255) COLLATE "pg_catalog"."default",
  "email" varchar(255) COLLATE "pg_catalog"."default",
  "kode_prov" int8,
  "kode_kotakab" varchar(255) COLLATE "pg_catalog"."default",
  "kode_kec" varchar(255) COLLATE "pg_catalog"."default",
  "kode_kel" varchar(255) COLLATE "pg_catalog"."default",
  "account_va" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "account_bank_name" varchar(255) COLLATE "pg_catalog"."default",
  "id_customer_lama" varchar(64) COLLATE "pg_catalog"."default",
  "id_customer_baru" varchar(64) COLLATE "pg_catalog"."default" NOT NULL,
  "id_increment" int4 NOT NULL DEFAULT nextval('master_customer_id_increment_seq'::regclass),
  "nama_btn" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for master_jenis_pembayaran
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_jenis_pembayaran";
CREATE TABLE "public"."master_jenis_pembayaran" (
  "id_pembayaran" int4 NOT NULL DEFAULT nextval('master_jenis_pembayaran_id_pembayaran_seq'::regclass),
  "pembayaran" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default" DEFAULT now(),
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false
)
;

-- ----------------------------
-- Table structure for master_kecamatan
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_kecamatan";
CREATE TABLE "public"."master_kecamatan" (
  "id" int8 NOT NULL,
  "kota_id" int8,
  "provinsi_id" int4,
  "nama" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "kode_kec" varchar(255) COLLATE "pg_catalog"."default",
  "kode_kotakab" varchar(255) COLLATE "pg_catalog"."default",
  "kode_prov" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false
)
;

-- ----------------------------
-- Table structure for master_kelurahan
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_kelurahan";
CREATE TABLE "public"."master_kelurahan" (
  "id" int8 NOT NULL,
  "kecamatan_id" int8 NOT NULL,
  "kota_id" int8,
  "provinsi_id" int4,
  "nama" varchar(40) COLLATE "pg_catalog"."default" NOT NULL,
  "kode_pos" int4 NOT NULL,
  "kode_kel" varchar(255) COLLATE "pg_catalog"."default",
  "kode_kec" varchar(255) COLLATE "pg_catalog"."default",
  "kode_kotakab" varchar(255) COLLATE "pg_catalog"."default",
  "kode_prov" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false
)
;

-- ----------------------------
-- Table structure for master_kotakab
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_kotakab";
CREATE TABLE "public"."master_kotakab" (
  "id" int8 NOT NULL,
  "provinsi_id" int4 NOT NULL,
  "level" varchar(30) COLLATE "pg_catalog"."default",
  "nama" varchar(255) COLLATE "pg_catalog"."default",
  "kode_kotakab" varchar(255) COLLATE "pg_catalog"."default",
  "kode_prov" varchar(255) COLLATE "pg_catalog"."default",
  "target_krs" int4,
  "status_view" int4,
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false
)
;

-- ----------------------------
-- Table structure for master_minimum_qty
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_minimum_qty";
CREATE TABLE "public"."master_minimum_qty" (
  "id" int4 NOT NULL DEFAULT nextval('master_minimum_qty_id_seq'::regclass),
  "qty" int4,
  "produk_id" int4,
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false,
  "updateindb" timestamp(6) DEFAULT now(),
  "min_max" varchar(255) COLLATE "pg_catalog"."default",
  "order_type" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for master_order_type
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_order_type";
CREATE TABLE "public"."master_order_type" (
  "order_type" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for master_pajak
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_pajak";
CREATE TABLE "public"."master_pajak" (
  "id_pajak" int4 NOT NULL DEFAULT nextval('master_pajak_id_pajak_seq'::regclass),
  "pajak" varchar(255) COLLATE "pg_catalog"."default",
  "presentase" numeric(32,1),
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false
)
;

-- ----------------------------
-- Table structure for master_produk
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_produk";
CREATE TABLE "public"."master_produk" (
  "id_produk" int4 NOT NULL DEFAULT nextval('master_produk_id_produk_seq'::regclass),
  "nama_produk" varchar(255) COLLATE "pg_catalog"."default",
  "deskripsi_produk" text COLLATE "pg_catalog"."default",
  "uom_satuan" int4,
  "kategori_produk" int4,
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false,
  "no_urut" int8,
  "kode_produk" varchar(255) COLLATE "pg_catalog"."default",
  "ppn" numeric(32,2),
  "pph22" numeric(32,2)
)
;

-- ----------------------------
-- Table structure for master_produk_kategori
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_produk_kategori";
CREATE TABLE "public"."master_produk_kategori" (
  "id_kategori" int4 NOT NULL DEFAULT nextval('master_kategori_id_kategori_seq'::regclass),
  "kategori" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar COLLATE "pg_catalog"."default",
  "uom_base" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for master_produk_uom_satuan
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_produk_uom_satuan";
CREATE TABLE "public"."master_produk_uom_satuan" (
  "id_uom_satuan" int4 NOT NULL DEFAULT nextval('master_uom_satuan_id_uom_satuan_seq'::regclass),
  "uom_satuan" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false,
  "userupdate" varchar COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "uom_base_convert" int4
)
;

-- ----------------------------
-- Table structure for master_provinsi
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_provinsi";
CREATE TABLE "public"."master_provinsi" (
  "id" int8 NOT NULL,
  "nama" varchar(30) COLLATE "pg_catalog"."default" NOT NULL,
  "kode" varchar(10) COLLATE "pg_catalog"."default" NOT NULL,
  "kode_prov" int8,
  "target_krs" int4,
  "status_view" int4 DEFAULT 0,
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false,
  "long" varchar(255) COLLATE "pg_catalog"."default",
  "lat" varchar(255) COLLATE "pg_catalog"."default",
  "unit_geografis" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for master_sales_price
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_sales_price";
CREATE TABLE "public"."master_sales_price" (
  "id_price" int4 NOT NULL DEFAULT nextval('master_sales_price_id_price_seq'::regclass),
  "id_produk" int4,
  "id_cabang" int4,
  "id_company" int4,
  "price" numeric(32,2),
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "order_type" varchar(255) COLLATE "pg_catalog"."default",
  "include_ppn" bool DEFAULT false
)
;
COMMENT ON COLUMN "public"."master_sales_price"."order_type" IS 'dropship, direct
';

-- ----------------------------
-- Table structure for master_template_download
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_template_download";
CREATE TABLE "public"."master_template_download" (
  "id" int4 NOT NULL DEFAULT nextval('master_template_download_id_seq'::regclass),
  "nama" varchar(255) COLLATE "pg_catalog"."default",
  "file_name" varchar(255) COLLATE "pg_catalog"."default",
  "kategori" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for master_unit_geografis
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_unit_geografis";
CREATE TABLE "public"."master_unit_geografis" (
  "id" varchar(32) COLLATE "pg_catalog"."default" NOT NULL,
  "color" varchar(255) COLLATE "pg_catalog"."default",
  "unit_geografis" varchar(255) COLLATE "pg_catalog"."default",
  "provinsi" varchar(255) COLLATE "pg_catalog"."default",
  "seq" int2
)
;

-- ----------------------------
-- Table structure for master_user
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_user";
CREATE TABLE "public"."master_user" (
  "id_user" int4 NOT NULL DEFAULT nextval('master_user_id_user_seq'::regclass),
  "username" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "password" text COLLATE "pg_catalog"."default",
  "user_role" int4,
  "company_id" int4,
  "cabang_id" int4,
  "status_release" bool DEFAULT false,
  "status_aktif" bool DEFAULT false,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "name" varchar(255) COLLATE "pg_catalog"."default",
  "is_salesman" bool,
  "is_view_only" bool DEFAULT false,
  "uuid" varchar(255) COLLATE "pg_catalog"."default" DEFAULT uuid_generate_v4()
)
;

-- ----------------------------
-- Table structure for master_user_role
-- ----------------------------
DROP TABLE IF EXISTS "public"."master_user_role";
CREATE TABLE "public"."master_user_role" (
  "id_role" int4 NOT NULL DEFAULT nextval('master_user_role_id_role_seq'::regclass),
  "role_name" varchar(255) COLLATE "pg_catalog"."default",
  "role_data" text COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool,
  "status_aktif" bool
)
;

-- ----------------------------
-- Table structure for trans_approval_detail
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_approval_detail";
CREATE TABLE "public"."trans_approval_detail" (
  "detail_id" int4 NOT NULL DEFAULT nextval('trans_approval_detail_detail_id_seq'::regclass),
  "header_id" varchar(255) COLLATE "pg_catalog"."default",
  "master_approval_id" int4,
  "order_approve" int4,
  "approval_status" int4,
  "approval_type" varchar(255) COLLATE "pg_catalog"."default",
  "action_time" timestamp(6),
  "description" text COLLATE "pg_catalog"."default",
  "issued_by" int4,
  "company_id" int4,
  "cabang_id" int4,
  "approval_trans" varchar(255) COLLATE "pg_catalog"."default",
  "active" bool DEFAULT true
)
;

-- ----------------------------
-- Table structure for trans_inventory_detail
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail";
CREATE TABLE "public"."trans_inventory_detail" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_bad_stock
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_bad_stock";
CREATE TABLE "public"."trans_inventory_detail_bad_stock" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default",
  "produk_id" int4,
  "qty" int4,
  "harga_satuan_hpp" numeric,
  "harga_total" numeric,
  "updateindb" timestamp(6),
  "userupdate" varchar COLLATE "pg_catalog"."default",
  "id_increment" int8 NOT NULL DEFAULT nextval('trans_inventory_detail_bad_stock_id_increment_seq'::regclass)
)
;

-- ----------------------------
-- Table structure for trans_inventory_detail_bad_stock_header
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_bad_stock_header";
CREATE TABLE "public"."trans_inventory_detail_bad_stock_header" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default",
  "id_increment" int8 NOT NULL DEFAULT nextval('trans_inventory_detail_bad_stock_header_id_increment_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4,
  "harga_total" numeric,
  "status_release" bool,
  "no_urut" int8,
  "updateindb" timestamp(6) DEFAULT now(),
  "tanggal" date,
  "approval_status" int4,
  "userupdate" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_inventory_detail_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_copy1";
CREATE TABLE "public"."trans_inventory_detail_copy1" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_inventory_detail_copy2
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_copy2";
CREATE TABLE "public"."trans_inventory_detail_copy2" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_copy2"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi";
CREATE TABLE "public"."trans_inventory_detail_mutasi" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy1";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy1" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy1"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy1"."mutasi_type" IS 'SO/TP/IDIN';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy10
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy10";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy10" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy10"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy10"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy10"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy11
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy11";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy11" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy11"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy11"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy11"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy12
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy12";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy12" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy12"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy12"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy12"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy13
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy13";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy13" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy13"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy13"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy13"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy14
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy14";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy14" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy14"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy14"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy14"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy15
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy15";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy15" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy15"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy15"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy15"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy16
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy16";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy16" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy16"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy16"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy16"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy17
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy17";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy17" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy17"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy17"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy17"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy18
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy18";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy18" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy18"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy18"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy18"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy19
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy19";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy19" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy19"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy19"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy19"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy2
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy2";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy2" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy2"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy2"."mutasi_type" IS 'SO/TP/IDIN';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy20
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy20";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy20" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy20"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy20"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy20"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy21
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy21";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy21" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy21"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy21"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy21"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy22
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy22";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy22" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy22"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy22"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy22"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy3
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy3";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy3" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy3"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy3"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy3"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy4
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy4";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy4" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy4"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy4"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy4"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy5
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy5";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy5" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy5"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy5"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy5"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy6
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy6";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy6" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy6"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy6"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy6"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy7
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy7";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy7" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy7"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy7"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy7"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy8
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy8";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy8" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy8"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy8"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy8"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy9
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy9";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy9" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy9"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy9"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy9"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy_03_16_20206
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy_03_16_20206";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy_03_16_20206" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy_03_16_20206"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy_03_16_20206"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy_03_16_20206"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_detail_mutasi_copy_03_16_20206_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_detail_mutasi_copy_03_16_20206_copy1";
CREATE TABLE "public"."trans_inventory_detail_mutasi_copy_03_16_20206_copy1" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_mutasi_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "in_out" varchar(255) COLLATE "pg_catalog"."default",
  "mutasi_type" varchar(255) COLLATE "pg_catalog"."default",
  "id_references" varchar(255) COLLATE "pg_catalog"."default",
  "tabel_reference" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date,
  "stock_condition" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'good'::character varying
)
;
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy_03_16_20206_copy1"."in_out" IS 'In/out';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy_03_16_20206_copy1"."mutasi_type" IS 'SO/TP/ST/DS/CRT';
COMMENT ON COLUMN "public"."trans_inventory_detail_mutasi_copy_03_16_20206_copy1"."stock_condition" IS 'good/bad';

-- ----------------------------
-- Table structure for trans_inventory_holding_delivery_preparation
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_holding_delivery_preparation";
CREATE TABLE "public"."trans_inventory_holding_delivery_preparation" (
  "id_trans" varchar(32) COLLATE "pg_catalog"."default" NOT NULL DEFAULT nextval('trans_inventory_holding_delivery_preparation_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool,
  "tanggal" date,
  "customer_id" int8,
  "harga_satuan_hpp" numeric(32,2),
  "harga_total_hpp" numeric(32,2),
  "id_increment" int8 NOT NULL DEFAULT nextval('trans_inventory_holding_delivery_preparation_id_increment_seq'::regclass),
  "transport_cost" numeric(32,2),
  "grand_total" numeric(32,2)
)
;

-- ----------------------------
-- Table structure for trans_inventory_holding_delivery_preparation_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_holding_delivery_preparation_copy1";
CREATE TABLE "public"."trans_inventory_holding_delivery_preparation_copy1" (
  "id_trans" varchar(32) COLLATE "pg_catalog"."default" NOT NULL DEFAULT nextval('trans_inventory_holding_delivery_preparation_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool,
  "tanggal" date,
  "customer_id" int8,
  "harga_satuan_hpp" numeric(32,2),
  "harga_total_hpp" numeric(32,2),
  "id_increment" int8 NOT NULL DEFAULT nextval('trans_inventory_holding_delivery_preparation_id_increment_seq'::regclass),
  "transport_cost" numeric(32,2),
  "grand_total" numeric(32,2)
)
;

-- ----------------------------
-- Table structure for trans_inventory_holding_delivery_preparation_header
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_holding_delivery_preparation_header";
CREATE TABLE "public"."trans_inventory_holding_delivery_preparation_header" (
  "id" int4 NOT NULL DEFAULT nextval('trans_inventory_holding_delivery_preparation_header_id_seq'::regclass),
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "company_id" int4,
  "cabang_id" int4,
  "salesman" int4,
  "tanggal" date,
  "customer_id" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "userupdate" varchar COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "harga_total" numeric(255,2),
  "order_type" varchar(255) COLLATE "pg_catalog"."default",
  "approval_status" int4,
  "description" text COLLATE "pg_catalog"."default",
  "transport_cost" numeric(32,2),
  "id_pembayaran" int4,
  "no_urut" int4,
  "id_trans_sales_order" varchar(255) COLLATE "pg_catalog"."default",
  "grand_total" numeric(32,2),
  "is_delivered" bool DEFAULT false,
  "tanggal_delivery" date
)
;
COMMENT ON COLUMN "public"."trans_inventory_holding_delivery_preparation_header"."order_type" IS 'direct, dropship';

-- ----------------------------
-- Table structure for trans_inventory_holding_delivery_preparation_header_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_holding_delivery_preparation_header_copy1";
CREATE TABLE "public"."trans_inventory_holding_delivery_preparation_header_copy1" (
  "id" int4 NOT NULL DEFAULT nextval('trans_inventory_holding_delivery_preparation_header_id_seq'::regclass),
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "company_id" int4,
  "cabang_id" int4,
  "salesman" int4,
  "tanggal" date,
  "customer_id" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "userupdate" varchar COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "harga_total" numeric(255,2),
  "order_type" varchar(255) COLLATE "pg_catalog"."default",
  "approval_status" int4,
  "description" text COLLATE "pg_catalog"."default",
  "transport_cost" numeric(32,2),
  "id_pembayaran" int4,
  "no_urut" int4,
  "id_trans_sales_order" varchar(255) COLLATE "pg_catalog"."default",
  "grand_total" numeric(32,2),
  "is_delivered" bool DEFAULT false,
  "tanggal_delivery" date
)
;
COMMENT ON COLUMN "public"."trans_inventory_holding_delivery_preparation_header_copy1"."order_type" IS 'direct, dropship';

-- ----------------------------
-- Table structure for trans_inventory_holding_delivery_preparation_header_copy2
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_holding_delivery_preparation_header_copy2";
CREATE TABLE "public"."trans_inventory_holding_delivery_preparation_header_copy2" (
  "id" int4 NOT NULL DEFAULT nextval('trans_inventory_holding_delivery_preparation_header_id_seq'::regclass),
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "company_id" int4,
  "cabang_id" int4,
  "salesman" int4,
  "tanggal" date,
  "customer_id" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "userupdate" varchar COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "harga_total" numeric(255,2),
  "order_type" varchar(255) COLLATE "pg_catalog"."default",
  "approval_status" int4,
  "description" text COLLATE "pg_catalog"."default",
  "transport_cost" numeric(32,2),
  "id_pembayaran" int4,
  "no_urut" int4,
  "id_trans_sales_order" varchar(255) COLLATE "pg_catalog"."default",
  "grand_total" numeric(32,2),
  "is_delivered" bool DEFAULT false,
  "tanggal_delivery" date
)
;
COMMENT ON COLUMN "public"."trans_inventory_holding_delivery_preparation_header_copy2"."order_type" IS 'direct, dropship';

-- ----------------------------
-- Table structure for trans_inventory_holding_submit
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_holding_submit";
CREATE TABLE "public"."trans_inventory_holding_submit" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "tanggal" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_holding_submit_id_increment_seq'::regclass)
)
;

-- ----------------------------
-- Table structure for trans_inventory_holding_submit_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_holding_submit_copy1";
CREATE TABLE "public"."trans_inventory_holding_submit_copy1" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "tanggal" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_holding_submit_id_increment_seq'::regclass)
)
;

-- ----------------------------
-- Table structure for trans_inventory_holding_transfer
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_holding_transfer";
CREATE TABLE "public"."trans_inventory_holding_transfer" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool,
  "tanggal" date,
  "to_company_id" int4,
  "to_cabang_id" int4,
  "transport_cost_total" numeric(32,2),
  "file_upload" text COLLATE "pg_catalog"."default",
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_holding_transfer_id_incement_seq'::regclass),
  "include_submit" bool DEFAULT false
)
;
COMMENT ON COLUMN "public"."trans_inventory_holding_transfer"."id_trans" IS 'Contoh:
HLD.IS.01.2025.08.0001
NUS.RO.01.2025.08.0001
NUS.SO.01.2025.08.0001
NUS.DO.01.2025.08.0001
NUS.INV.01.2025.08.0001';

-- ----------------------------
-- Table structure for trans_inventory_holding_transfer_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_holding_transfer_copy1";
CREATE TABLE "public"."trans_inventory_holding_transfer_copy1" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool,
  "tanggal" date,
  "to_company_id" int4,
  "to_cabang_id" int4,
  "transport_cost_total" numeric(32,2),
  "file_upload" text COLLATE "pg_catalog"."default",
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_holding_transfer_id_incement_seq'::regclass)
)
;
COMMENT ON COLUMN "public"."trans_inventory_holding_transfer_copy1"."id_trans" IS 'Contoh:
HLD.IS.01.2025.08.0001
NUS.RO.01.2025.08.0001
NUS.SO.01.2025.08.0001
NUS.DO.01.2025.08.0001
NUS.INV.01.2025.08.0001';

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_delivery_order
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_delivery_order";
CREATE TABLE "public"."trans_inventory_subsidiary_delivery_order" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool,
  "tanggal_do" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_trans_sales_order" varchar(255) COLLATE "pg_catalog"."default",
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_delivery_order_id_increment_seq'::regclass)
)
;

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_invoice
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_invoice";
CREATE TABLE "public"."trans_inventory_subsidiary_invoice" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT true,
  "tanggal_invoice" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_trans_sales_order" varchar(255) COLLATE "pg_catalog"."default",
  "id_trans_delivery_order" varchar(255) COLLATE "pg_catalog"."default",
  "status_invoice" bool DEFAULT true,
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_invoice_id_increment_seq'::regclass),
  "id_sales_report" varchar(255) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  "tanggal_due_date" date,
  "amount" numeric(32,2),
  "amount_ppn" numeric(32,2),
  "amount_pph" numeric(32,2),
  "amount_total" numeric(32,2),
  "complete_payment" bool DEFAULT false,
  "amount_total_outstanding" numeric(32,2),
  "customer_id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "md5_file" varchar(255) COLLATE "pg_catalog"."default",
  "salesman" int4,
  "id_pembayaran" int4,
  "customer_id_lama" int4,
  "customer_id_baru" int8,
  "payment_last_updated" date,
  "biaya_admin" numeric,
  "reference_pre_payment" varchar(255) COLLATE "pg_catalog"."default",
  "desc" varchar(255) COLLATE "pg_catalog"."default",
  "not_va" bool DEFAULT false,
  "shared_margin" bool DEFAULT false
)
;
COMMENT ON COLUMN "public"."trans_inventory_subsidiary_invoice"."status_invoice" IS 'Pending/progress/done';

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_invoice_before_perbaikan
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_invoice_before_perbaikan";
CREATE TABLE "public"."trans_inventory_subsidiary_invoice_before_perbaikan" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT true,
  "tanggal_invoice" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_trans_sales_order" varchar(255) COLLATE "pg_catalog"."default",
  "id_trans_delivery_order" varchar(255) COLLATE "pg_catalog"."default",
  "status_invoice" bool DEFAULT true,
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_invoice_id_increment_seq'::regclass),
  "id_sales_report" varchar(255) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  "tanggal_due_date" date,
  "amount" numeric(32,2),
  "amount_ppn" numeric(32,2),
  "amount_pph" numeric(32,2),
  "amount_total" numeric(32,2),
  "complete_payment" bool DEFAULT false,
  "amount_total_outstanding" numeric(32,2),
  "customer_id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "md5_file" varchar(255) COLLATE "pg_catalog"."default",
  "salesman" int4,
  "id_pembayaran" int4,
  "customer_id_lama" int4,
  "customer_id_baru" int8,
  "payment_last_updated" date,
  "biaya_admin" numeric,
  "reference_pre_payment" varchar(255) COLLATE "pg_catalog"."default",
  "desc" varchar(255) COLLATE "pg_catalog"."default",
  "not_va" bool DEFAULT false,
  "shared_margin" bool DEFAULT false
)
;
COMMENT ON COLUMN "public"."trans_inventory_subsidiary_invoice_before_perbaikan"."status_invoice" IS 'Pending/progress/done';

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_invoice_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_invoice_copy1";
CREATE TABLE "public"."trans_inventory_subsidiary_invoice_copy1" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT true,
  "tanggal_invoice" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_trans_sales_order" varchar(255) COLLATE "pg_catalog"."default",
  "id_trans_delivery_order" varchar(255) COLLATE "pg_catalog"."default",
  "status_invoice" bool DEFAULT true,
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_invoice_id_increment_seq'::regclass),
  "id_sales_report" varchar(255) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  "tanggal_due_date" date,
  "amount" numeric(32,2),
  "amount_ppn" numeric(32,2),
  "amount_pph" numeric(32,2),
  "amount_total" numeric(32,2),
  "complete_payment" bool DEFAULT false,
  "amount_total_outstanding" numeric(32,2),
  "customer_id" int8 NOT NULL,
  "md5_file" varchar(255) COLLATE "pg_catalog"."default",
  "salesman" int4,
  "id_pembayaran" int4,
  "customer_id_lama" int4,
  "customer_id_baru" int8,
  "payment_last_updated" date,
  "biaya_admin" numeric,
  "reference_pre_payment" varchar(255) COLLATE "pg_catalog"."default"
)
;
COMMENT ON COLUMN "public"."trans_inventory_subsidiary_invoice_copy1"."status_invoice" IS 'Pending/progress/done';

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_invoice_copy2
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_invoice_copy2";
CREATE TABLE "public"."trans_inventory_subsidiary_invoice_copy2" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT true,
  "tanggal_invoice" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_trans_sales_order" varchar(255) COLLATE "pg_catalog"."default",
  "id_trans_delivery_order" varchar(255) COLLATE "pg_catalog"."default",
  "status_invoice" bool DEFAULT true,
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_invoice_id_increment_seq'::regclass),
  "id_sales_report" varchar(255) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  "tanggal_due_date" date,
  "amount" numeric(32,2),
  "amount_ppn" numeric(32,2),
  "amount_pph" numeric(32,2),
  "amount_total" numeric(32,2),
  "complete_payment" bool DEFAULT false,
  "amount_total_outstanding" numeric(32,2),
  "customer_id" int8 NOT NULL,
  "md5_file" varchar(255) COLLATE "pg_catalog"."default",
  "salesman" int4,
  "id_pembayaran" int4,
  "customer_id_lama" int4,
  "customer_id_baru" int8,
  "payment_last_updated" date,
  "biaya_admin" numeric,
  "reference_pre_payment" varchar(255) COLLATE "pg_catalog"."default",
  "desc" varchar(255) COLLATE "pg_catalog"."default"
)
;
COMMENT ON COLUMN "public"."trans_inventory_subsidiary_invoice_copy2"."status_invoice" IS 'Pending/progress/done';

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_invoice_copy3
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_invoice_copy3";
CREATE TABLE "public"."trans_inventory_subsidiary_invoice_copy3" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT true,
  "tanggal_invoice" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_trans_sales_order" varchar(255) COLLATE "pg_catalog"."default",
  "id_trans_delivery_order" varchar(255) COLLATE "pg_catalog"."default",
  "status_invoice" bool DEFAULT true,
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_invoice_id_increment_seq'::regclass),
  "id_sales_report" varchar(255) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  "tanggal_due_date" date,
  "amount" numeric(32,2),
  "amount_ppn" numeric(32,2),
  "amount_pph" numeric(32,2),
  "amount_total" numeric(32,2),
  "complete_payment" bool DEFAULT false,
  "amount_total_outstanding" numeric(32,2),
  "customer_id" int8 NOT NULL,
  "md5_file" varchar(255) COLLATE "pg_catalog"."default",
  "salesman" int4,
  "id_pembayaran" int4,
  "customer_id_lama" int4,
  "customer_id_baru" int8,
  "payment_last_updated" date,
  "biaya_admin" numeric,
  "reference_pre_payment" varchar(255) COLLATE "pg_catalog"."default",
  "desc" varchar(255) COLLATE "pg_catalog"."default",
  "not_va" bool DEFAULT false
)
;
COMMENT ON COLUMN "public"."trans_inventory_subsidiary_invoice_copy3"."status_invoice" IS 'Pending/progress/done';

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_invoice_copy4
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_invoice_copy4";
CREATE TABLE "public"."trans_inventory_subsidiary_invoice_copy4" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT true,
  "tanggal_invoice" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_trans_sales_order" varchar(255) COLLATE "pg_catalog"."default",
  "id_trans_delivery_order" varchar(255) COLLATE "pg_catalog"."default",
  "status_invoice" bool DEFAULT true,
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_invoice_id_increment_seq'::regclass),
  "id_sales_report" varchar(255) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  "tanggal_due_date" date,
  "amount" numeric(32,2),
  "amount_ppn" numeric(32,2),
  "amount_pph" numeric(32,2),
  "amount_total" numeric(32,2),
  "complete_payment" bool DEFAULT false,
  "amount_total_outstanding" numeric(32,2),
  "customer_id" int8 NOT NULL,
  "md5_file" varchar(255) COLLATE "pg_catalog"."default",
  "salesman" int4,
  "id_pembayaran" int4,
  "customer_id_lama" int4,
  "customer_id_baru" int8,
  "payment_last_updated" date,
  "biaya_admin" numeric,
  "reference_pre_payment" varchar(255) COLLATE "pg_catalog"."default",
  "desc" varchar(255) COLLATE "pg_catalog"."default",
  "not_va" bool DEFAULT false
)
;
COMMENT ON COLUMN "public"."trans_inventory_subsidiary_invoice_copy4"."status_invoice" IS 'Pending/progress/done';

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_invoice_copy5
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_invoice_copy5";
CREATE TABLE "public"."trans_inventory_subsidiary_invoice_copy5" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT true,
  "tanggal_invoice" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_trans_sales_order" varchar(255) COLLATE "pg_catalog"."default",
  "id_trans_delivery_order" varchar(255) COLLATE "pg_catalog"."default",
  "status_invoice" bool DEFAULT true,
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_invoice_id_increment_seq'::regclass),
  "id_sales_report" varchar(255) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  "tanggal_due_date" date,
  "amount" numeric(32,2),
  "amount_ppn" numeric(32,2),
  "amount_pph" numeric(32,2),
  "amount_total" numeric(32,2),
  "complete_payment" bool DEFAULT false,
  "amount_total_outstanding" numeric(32,2),
  "customer_id" int8 NOT NULL,
  "md5_file" varchar(255) COLLATE "pg_catalog"."default",
  "salesman" int4,
  "id_pembayaran" int4,
  "customer_id_lama" int4,
  "customer_id_baru" int8,
  "payment_last_updated" date,
  "biaya_admin" numeric,
  "reference_pre_payment" varchar(255) COLLATE "pg_catalog"."default",
  "desc" varchar(255) COLLATE "pg_catalog"."default",
  "not_va" bool DEFAULT false,
  "shared_margin" bool DEFAULT false
)
;
COMMENT ON COLUMN "public"."trans_inventory_subsidiary_invoice_copy5"."status_invoice" IS 'Pending/progress/done';

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_invoice_deleted
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_invoice_deleted";
CREATE TABLE "public"."trans_inventory_subsidiary_invoice_deleted" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT true,
  "tanggal_invoice" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_trans_sales_order" varchar(255) COLLATE "pg_catalog"."default",
  "id_trans_delivery_order" varchar(255) COLLATE "pg_catalog"."default",
  "status_invoice" bool DEFAULT true,
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_invoice_id_increment_seq'::regclass),
  "id_sales_report" varchar(255) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  "tanggal_due_date" date,
  "amount" numeric(32,2),
  "amount_ppn" numeric(32,2),
  "amount_pph" numeric(32,2),
  "amount_total" numeric(32,2),
  "complete_payment" bool DEFAULT false,
  "amount_total_outstanding" numeric(32,2),
  "customer_id" int8 NOT NULL,
  "md5_file" varchar(255) COLLATE "pg_catalog"."default",
  "salesman" int4,
  "id_pembayaran" int4,
  "customer_id_lama" int4,
  "customer_id_baru" int8,
  "payment_last_updated" date,
  "biaya_admin" numeric,
  "reference_pre_payment" varchar(255) COLLATE "pg_catalog"."default",
  "desc" varchar(255) COLLATE "pg_catalog"."default",
  "not_va" bool DEFAULT false,
  "shared_margin" bool DEFAULT false
)
;
COMMENT ON COLUMN "public"."trans_inventory_subsidiary_invoice_deleted"."status_invoice" IS 'Pending/progress/done';

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_invoice_pre_payment
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_invoice_pre_payment";
CREATE TABLE "public"."trans_inventory_subsidiary_invoice_pre_payment" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT true,
  "tanggal_invoice" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_trans_sales_order" varchar(255) COLLATE "pg_catalog"."default",
  "id_trans_delivery_order" varchar(255) COLLATE "pg_catalog"."default",
  "status_invoice" bool DEFAULT true,
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_invoice_id_increment_seq'::regclass),
  "id_sales_report" varchar(255) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  "tanggal_due_date" date,
  "amount" numeric(32,2),
  "amount_ppn" numeric(32,2),
  "amount_pph" numeric(32,2),
  "amount_total" numeric(32,2),
  "complete_payment" bool DEFAULT false,
  "amount_total_outstanding" numeric(32,2),
  "customer_id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "md5_file" varchar(255) COLLATE "pg_catalog"."default",
  "salesman" int4,
  "id_pembayaran" int4,
  "customer_id_lama" int4,
  "customer_id_baru" int8,
  "payment_last_updated" date,
  "biaya_admin" numeric,
  "is_canceled" bool DEFAULT false
)
;
COMMENT ON COLUMN "public"."trans_inventory_subsidiary_invoice_pre_payment"."status_invoice" IS 'Pending/progress/done';

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_invoice_pre_payment_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_invoice_pre_payment_copy1";
CREATE TABLE "public"."trans_inventory_subsidiary_invoice_pre_payment_copy1" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT true,
  "tanggal_invoice" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_trans_sales_order" varchar(255) COLLATE "pg_catalog"."default",
  "id_trans_delivery_order" varchar(255) COLLATE "pg_catalog"."default",
  "status_invoice" bool DEFAULT true,
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_invoice_id_increment_seq'::regclass),
  "id_sales_report" varchar(255) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  "tanggal_due_date" date,
  "amount" numeric(32,2),
  "amount_ppn" numeric(32,2),
  "amount_pph" numeric(32,2),
  "amount_total" numeric(32,2),
  "complete_payment" bool DEFAULT false,
  "amount_total_outstanding" numeric(32,2),
  "customer_id" int8 NOT NULL,
  "md5_file" varchar(255) COLLATE "pg_catalog"."default",
  "salesman" int4,
  "id_pembayaran" int4,
  "customer_id_lama" int4,
  "customer_id_baru" int8,
  "payment_last_updated" date,
  "biaya_admin" numeric,
  "is_canceled" bool DEFAULT false
)
;
COMMENT ON COLUMN "public"."trans_inventory_subsidiary_invoice_pre_payment_copy1"."status_invoice" IS 'Pending/progress/done';

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_receipt_transfer
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_receipt_transfer";
CREATE TABLE "public"."trans_inventory_subsidiary_receipt_transfer" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "company_id" int4,
  "cabang_id" int4,
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "tanggal" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_trans_holding_transfer" varchar(255) COLLATE "pg_catalog"."default",
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_receipt_transfer_id_increment_seq'::regclass),
  "tanggal_receipt" timestamp(6)
)
;

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_receipt_transfer_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_receipt_transfer_copy1";
CREATE TABLE "public"."trans_inventory_subsidiary_receipt_transfer_copy1" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "company_id" int4,
  "cabang_id" int4,
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "tanggal" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_trans_holding_transfer" varchar(255) COLLATE "pg_catalog"."default",
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_receipt_transfer_id_increment_seq'::regclass),
  "tanggal_receipt" timestamp(6)
)
;

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_receipt_transfer_copy2
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_receipt_transfer_copy2";
CREATE TABLE "public"."trans_inventory_subsidiary_receipt_transfer_copy2" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "company_id" int4,
  "cabang_id" int4,
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool DEFAULT false,
  "tanggal" date,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_trans_holding_transfer" varchar(255) COLLATE "pg_catalog"."default",
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_receipt_transfer_id_increment_seq'::regclass),
  "tanggal_receipt" timestamp(6)
)
;

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_retur_detail
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_retur_detail";
CREATE TABLE "public"."trans_inventory_subsidiary_retur_detail" (
  "id_detail" int4 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_retur_detail_id_detail_seq'::regclass),
  "id_header" varchar(255) COLLATE "pg_catalog"."default",
  "qty_retur" int8,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "produk_id" int4,
  "qty_order" int8
)
;

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_retur_header
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_retur_header";
CREATE TABLE "public"."trans_inventory_subsidiary_retur_header" (
  "id_header" varchar(255) COLLATE "pg_catalog"."default",
  "id_invoice" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal_retur" date,
  "status_release" bool,
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "no_urut" int4,
  "id_increment" int8 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_retur_header_id_increment_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4
)
;

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_sales_order
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_sales_order";
CREATE TABLE "public"."trans_inventory_subsidiary_sales_order" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool,
  "tanggal" date,
  "customer_id" varchar(255) COLLATE "pg_catalog"."default",
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_produk_harga_jual" int4,
  "ppn_percent" numeric,
  "ppn_value" numeric(64,2),
  "pph_22_percent" numeric(64,2),
  "pph_22_value" numeric(64,2),
  "harga_total_ppn_pph" numeric(32,2),
  "no_urut" int4,
  "id_increment" int8 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_sales_order_id_increment_seq'::regclass),
  "harga_satuan_hpp" numeric(32,2),
  "harga_total_hpp" numeric(32,2),
  "flag_sales_report" bool DEFAULT false,
  "salesman" int4,
  "id_pembayaran" int4,
  "biaya_admin" numeric(32,2),
  "customer_id_lama" int4,
  "customer_id_baru" int8
)
;

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_sales_order_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_sales_order_copy1";
CREATE TABLE "public"."trans_inventory_subsidiary_sales_order_copy1" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool,
  "tanggal" date,
  "customer_id" int8,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_produk_harga_jual" int4,
  "ppn_percent" numeric,
  "ppn_value" numeric(64,2),
  "pph_22_percent" numeric,
  "pph_22_value" numeric(64,2),
  "harga_total_ppn_pph" numeric(32,2),
  "no_urut" int4,
  "id_increment" int8 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_sales_order_id_increment_seq'::regclass),
  "harga_satuan_hpp" numeric(32,2),
  "harga_total_hpp" numeric(32,2),
  "flag_sales_report" bool DEFAULT false,
  "salesman" int4,
  "id_pembayaran" int4,
  "biaya_admin" numeric(32,2),
  "customer_id_lama" int4,
  "customer_id_baru" int8
)
;

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_sales_order_copy2
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_sales_order_copy2";
CREATE TABLE "public"."trans_inventory_subsidiary_sales_order_copy2" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool,
  "tanggal" date,
  "customer_id" int8,
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_produk_harga_jual" int4,
  "ppn_percent" numeric,
  "ppn_value" numeric(64,2),
  "pph_22_percent" numeric,
  "pph_22_value" numeric(64,2),
  "harga_total_ppn_pph" numeric(32,2),
  "no_urut" int4,
  "id_increment" int8 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_sales_order_id_increment_seq'::regclass),
  "harga_satuan_hpp" numeric(32,2),
  "harga_total_hpp" numeric(32,2),
  "flag_sales_report" bool DEFAULT false,
  "salesman" int4,
  "id_pembayaran" int4,
  "biaya_admin" numeric(32,2),
  "customer_id_lama" int4,
  "customer_id_baru" int8
)
;

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_sales_order_copy3
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_sales_order_copy3";
CREATE TABLE "public"."trans_inventory_subsidiary_sales_order_copy3" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool,
  "tanggal" date,
  "customer_id" varchar(255) COLLATE "pg_catalog"."default",
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_produk_harga_jual" int4,
  "ppn_percent" numeric,
  "ppn_value" numeric(64,2),
  "pph_22_percent" numeric,
  "pph_22_value" numeric(64,2),
  "harga_total_ppn_pph" numeric(32,2),
  "no_urut" int4,
  "id_increment" int8 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_sales_order_id_increment_seq'::regclass),
  "harga_satuan_hpp" numeric(32,2),
  "harga_total_hpp" numeric(32,2),
  "flag_sales_report" bool DEFAULT false,
  "salesman" int4,
  "id_pembayaran" int4,
  "biaya_admin" numeric(32,2),
  "customer_id_lama" int4,
  "customer_id_baru" int8
)
;

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_sales_order_copy4
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_sales_order_copy4";
CREATE TABLE "public"."trans_inventory_subsidiary_sales_order_copy4" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool,
  "tanggal" date,
  "customer_id" varchar(255) COLLATE "pg_catalog"."default",
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_produk_harga_jual" int4,
  "ppn_percent" numeric,
  "ppn_value" numeric(64,2),
  "pph_22_percent" numeric,
  "pph_22_value" numeric(64,2),
  "harga_total_ppn_pph" numeric(32,2),
  "no_urut" int4,
  "id_increment" int8 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_sales_order_id_increment_seq'::regclass),
  "harga_satuan_hpp" numeric(32,2),
  "harga_total_hpp" numeric(32,2),
  "flag_sales_report" bool DEFAULT false,
  "salesman" int4,
  "id_pembayaran" int4,
  "biaya_admin" numeric(32,2),
  "customer_id_lama" int4,
  "customer_id_baru" int8
)
;

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_sales_order_copy_before_perbaikan
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_sales_order_copy_before_perbaikan";
CREATE TABLE "public"."trans_inventory_subsidiary_sales_order_copy_before_perbaikan" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "status_release" bool,
  "tanggal" date,
  "customer_id" varchar(255) COLLATE "pg_catalog"."default",
  "file_upload" text COLLATE "pg_catalog"."default",
  "id_produk_harga_jual" int4,
  "ppn_percent" numeric,
  "ppn_value" numeric(64,2),
  "pph_22_percent" numeric,
  "pph_22_value" numeric(64,2),
  "harga_total_ppn_pph" numeric(32,2),
  "no_urut" int4,
  "id_increment" int8 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_sales_order_id_increment_seq'::regclass),
  "harga_satuan_hpp" numeric(32,2),
  "harga_total_hpp" numeric(32,2),
  "flag_sales_report" bool DEFAULT false,
  "salesman" int4,
  "id_pembayaran" int4,
  "biaya_admin" numeric(32,2),
  "customer_id_lama" int4,
  "customer_id_baru" int8
)
;

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_sales_order_header
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_sales_order_header";
CREATE TABLE "public"."trans_inventory_subsidiary_sales_order_header" (
  "id" int8 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_sales_order_header_id_seq'::regclass),
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "no_urut" int4 NOT NULL,
  "company_id" int4,
  "cabang_id" int4,
  "salesman" int4,
  "tanggal" date,
  "customer_id" varchar(255) COLLATE "pg_catalog"."default",
  "id_pembayaran" int4,
  "total_ppn" numeric(255,2),
  "total_pph" numeric(255,2),
  "harga_total_hpp" numeric(255,2),
  "biaya_admin" numeric(255,2),
  "harga_total_ppn_pph" numeric(255,2),
  "flag_sales_report" bool DEFAULT false,
  "status_release" bool DEFAULT false,
  "userupdate" varchar COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "harga_total" numeric(255,2),
  "order_type" varchar(255) COLLATE "pg_catalog"."default",
  "approval_status" int4,
  "description" text COLLATE "pg_catalog"."default"
)
;
COMMENT ON COLUMN "public"."trans_inventory_subsidiary_sales_order_header"."order_type" IS 'direct, dropship';

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_sales_order_header_before_perbaikan
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_sales_order_header_before_perbaikan";
CREATE TABLE "public"."trans_inventory_subsidiary_sales_order_header_before_perbaikan" (
  "id" int8 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_sales_order_header_id_seq'::regclass),
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "no_urut" int4 NOT NULL,
  "company_id" int4,
  "cabang_id" int4,
  "salesman" int4,
  "tanggal" date,
  "customer_id" varchar(255) COLLATE "pg_catalog"."default",
  "id_pembayaran" int4,
  "total_ppn" numeric(255,2),
  "total_pph" numeric(255,2),
  "harga_total_hpp" numeric(255,2),
  "biaya_admin" numeric(255,2),
  "harga_total_ppn_pph" numeric(255,2),
  "flag_sales_report" bool DEFAULT false,
  "status_release" bool DEFAULT false,
  "userupdate" varchar COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "harga_total" numeric(255,2),
  "order_type" varchar(255) COLLATE "pg_catalog"."default",
  "approval_status" int4,
  "description" text COLLATE "pg_catalog"."default"
)
;
COMMENT ON COLUMN "public"."trans_inventory_subsidiary_sales_order_header_before_perbaikan"."order_type" IS 'direct, dropship';

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_sales_order_header_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_sales_order_header_copy1";
CREATE TABLE "public"."trans_inventory_subsidiary_sales_order_header_copy1" (
  "id" int8 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_sales_order_header_id_seq'::regclass),
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "no_urut" int4 NOT NULL,
  "company_id" int4,
  "cabang_id" int4,
  "salesman" int4,
  "tanggal" date,
  "customer_id" int8,
  "id_pembayaran" int4,
  "total_ppn" numeric(255,2),
  "total_pph" numeric(255,2),
  "harga_total_hpp" numeric(255,2),
  "biaya_admin" numeric(255,2),
  "harga_total_ppn_pph" numeric(255,2),
  "flag_sales_report" bool DEFAULT false,
  "status_release" bool DEFAULT false,
  "userupdate" varchar COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "harga_total" numeric(255,2),
  "order_type" varchar(255) COLLATE "pg_catalog"."default",
  "approval_status" int4,
  "description" text COLLATE "pg_catalog"."default"
)
;
COMMENT ON COLUMN "public"."trans_inventory_subsidiary_sales_order_header_copy1"."order_type" IS 'direct, dropship';

-- ----------------------------
-- Table structure for trans_inventory_subsidiary_sales_order_header_copy2
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_inventory_subsidiary_sales_order_header_copy2";
CREATE TABLE "public"."trans_inventory_subsidiary_sales_order_header_copy2" (
  "id" int8 NOT NULL DEFAULT nextval('trans_inventory_subsidiary_sales_order_header_id_seq'::regclass),
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "no_urut" int4 NOT NULL,
  "company_id" int4,
  "cabang_id" int4,
  "salesman" int4,
  "tanggal" date,
  "customer_id" varchar(255) COLLATE "pg_catalog"."default",
  "id_pembayaran" int4,
  "total_ppn" numeric(255,2),
  "total_pph" numeric(255,2),
  "harga_total_hpp" numeric(255,2),
  "biaya_admin" numeric(255,2),
  "harga_total_ppn_pph" numeric(255,2),
  "flag_sales_report" bool DEFAULT false,
  "status_release" bool DEFAULT false,
  "userupdate" varchar COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "harga_total" numeric(255,2),
  "order_type" varchar(255) COLLATE "pg_catalog"."default",
  "approval_status" int4,
  "description" text COLLATE "pg_catalog"."default"
)
;
COMMENT ON COLUMN "public"."trans_inventory_subsidiary_sales_order_header_copy2"."order_type" IS 'direct, dropship';

-- ----------------------------
-- Table structure for trans_produk_harga_jual
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_produk_harga_jual";
CREATE TABLE "public"."trans_produk_harga_jual" (
  "id_trans" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "id_produk" int4,
  "harga_produk" int8,
  "status_release" bool,
  "default" bool,
  "updateindb" timestamp(6),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "company_id" int4,
  "cabang_id" int4,
  "no_urut" int4,
  "id_increment" int4 NOT NULL DEFAULT nextval('trans_produk_harga_jual_id_increment_seq'::regclass)
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment";
CREATE TABLE "public"."trans_sales_order_paid_payment" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_id_trans_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4,
  "customer_id" int4,
  "virtual_account" varchar(255) COLLATE "pg_catalog"."default",
  "nominal" numeric(32,2),
  "invoice" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date DEFAULT now(),
  "tanggal_full" timestamp(6),
  "trans_type" varchar(255) COLLATE "pg_catalog"."default",
  "rekon_status" bool DEFAULT false,
  "name" varchar(255) COLLATE "pg_catalog"."default",
  "deskripsi" text COLLATE "pg_catalog"."default",
  "wrong_va" bool DEFAULT false,
  "bank_type" int2 DEFAULT 1,
  "rek_type" varchar(2) COLLATE "pg_catalog"."default" DEFAULT 'va'::character varying
)
;
COMMENT ON COLUMN "public"."trans_sales_order_paid_payment"."bank_type" IS '1. BTN VA, 2. Mandiri, 3.BTN RNI';
COMMENT ON COLUMN "public"."trans_sales_order_paid_payment"."rek_type" IS 'VA - Virutal Account / RA- regular account';

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_copy1";
CREATE TABLE "public"."trans_sales_order_paid_payment_copy1" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_id_trans_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4,
  "customer_id" int4,
  "virtual_account" varchar(255) COLLATE "pg_catalog"."default",
  "nominal" numeric(32,2),
  "invoice" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date DEFAULT now(),
  "tanggal_full" timestamp(6),
  "trans_type" varchar(255) COLLATE "pg_catalog"."default",
  "rekon_status" bool DEFAULT false,
  "name" varchar(255) COLLATE "pg_catalog"."default",
  "deskripsi" text COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_copy2
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_copy2";
CREATE TABLE "public"."trans_sales_order_paid_payment_copy2" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_id_trans_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4,
  "customer_id" int4,
  "virtual_account" varchar(255) COLLATE "pg_catalog"."default",
  "nominal" numeric(32,2),
  "invoice" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date DEFAULT now(),
  "tanggal_full" timestamp(6),
  "trans_type" varchar(255) COLLATE "pg_catalog"."default",
  "rekon_status" bool DEFAULT false,
  "name" varchar(255) COLLATE "pg_catalog"."default",
  "deskripsi" text COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_copy3
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_copy3";
CREATE TABLE "public"."trans_sales_order_paid_payment_copy3" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_id_trans_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4,
  "customer_id" int4,
  "virtual_account" varchar(255) COLLATE "pg_catalog"."default",
  "nominal" numeric(32,2),
  "invoice" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date DEFAULT now(),
  "tanggal_full" timestamp(6),
  "trans_type" varchar(255) COLLATE "pg_catalog"."default",
  "rekon_status" bool DEFAULT false,
  "name" varchar(255) COLLATE "pg_catalog"."default",
  "deskripsi" text COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_copy4
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_copy4";
CREATE TABLE "public"."trans_sales_order_paid_payment_copy4" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_id_trans_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4,
  "customer_id" int4,
  "virtual_account" varchar(255) COLLATE "pg_catalog"."default",
  "nominal" numeric(32,2),
  "invoice" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date DEFAULT now(),
  "tanggal_full" timestamp(6),
  "trans_type" varchar(255) COLLATE "pg_catalog"."default",
  "rekon_status" bool DEFAULT false,
  "name" varchar(255) COLLATE "pg_catalog"."default",
  "deskripsi" text COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_copy5
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_copy5";
CREATE TABLE "public"."trans_sales_order_paid_payment_copy5" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_id_trans_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4,
  "customer_id" int4,
  "virtual_account" varchar(255) COLLATE "pg_catalog"."default",
  "nominal" numeric(32,2),
  "invoice" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date DEFAULT now(),
  "tanggal_full" timestamp(6),
  "trans_type" varchar(255) COLLATE "pg_catalog"."default",
  "rekon_status" bool DEFAULT false,
  "name" varchar(255) COLLATE "pg_catalog"."default",
  "deskripsi" text COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_copy6
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_copy6";
CREATE TABLE "public"."trans_sales_order_paid_payment_copy6" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_id_trans_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4,
  "customer_id" int4,
  "virtual_account" varchar(255) COLLATE "pg_catalog"."default",
  "nominal" numeric(32,2),
  "invoice" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date DEFAULT now(),
  "tanggal_full" timestamp(6),
  "trans_type" varchar(255) COLLATE "pg_catalog"."default",
  "rekon_status" bool DEFAULT false,
  "name" varchar(255) COLLATE "pg_catalog"."default",
  "deskripsi" text COLLATE "pg_catalog"."default",
  "wrong_va" bool DEFAULT false,
  "bank_type" int2 DEFAULT 1,
  "rek_type" varchar(2) COLLATE "pg_catalog"."default" DEFAULT 'va'::character varying
)
;
COMMENT ON COLUMN "public"."trans_sales_order_paid_payment_copy6"."bank_type" IS '1. BTN, 2. Mandiri, 3. setgerusnya';
COMMENT ON COLUMN "public"."trans_sales_order_paid_payment_copy6"."rek_type" IS 'VA - Virutal Account / RA- regular account';

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_copy7
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_copy7";
CREATE TABLE "public"."trans_sales_order_paid_payment_copy7" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_id_trans_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4,
  "customer_id" int4,
  "virtual_account" varchar(255) COLLATE "pg_catalog"."default",
  "nominal" numeric(32,2),
  "invoice" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date DEFAULT now(),
  "tanggal_full" timestamp(6),
  "trans_type" varchar(255) COLLATE "pg_catalog"."default",
  "rekon_status" bool DEFAULT false,
  "name" varchar(255) COLLATE "pg_catalog"."default",
  "deskripsi" text COLLATE "pg_catalog"."default",
  "wrong_va" bool DEFAULT false,
  "bank_type" int2 DEFAULT 1,
  "rek_type" varchar(2) COLLATE "pg_catalog"."default" DEFAULT 'va'::character varying
)
;
COMMENT ON COLUMN "public"."trans_sales_order_paid_payment_copy7"."bank_type" IS '1. BTN VA, 2. Mandiri, 3.BTN RNI';
COMMENT ON COLUMN "public"."trans_sales_order_paid_payment_copy7"."rek_type" IS 'VA - Virutal Account / RA- regular account';

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_copy8
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_copy8";
CREATE TABLE "public"."trans_sales_order_paid_payment_copy8" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_id_trans_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4,
  "customer_id" int4,
  "virtual_account" varchar(255) COLLATE "pg_catalog"."default",
  "nominal" numeric(32,2),
  "invoice" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date DEFAULT now(),
  "tanggal_full" timestamp(6),
  "trans_type" varchar(255) COLLATE "pg_catalog"."default",
  "rekon_status" bool DEFAULT false,
  "name" varchar(255) COLLATE "pg_catalog"."default",
  "deskripsi" text COLLATE "pg_catalog"."default",
  "wrong_va" bool DEFAULT false,
  "bank_type" int2 DEFAULT 1,
  "rek_type" varchar(2) COLLATE "pg_catalog"."default" DEFAULT 'va'::character varying
)
;
COMMENT ON COLUMN "public"."trans_sales_order_paid_payment_copy8"."bank_type" IS '1. BTN VA, 2. Mandiri, 3.BTN RNI';
COMMENT ON COLUMN "public"."trans_sales_order_paid_payment_copy8"."rek_type" IS 'VA - Virutal Account / RA- regular account';

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_copy_backup
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_copy_backup";
CREATE TABLE "public"."trans_sales_order_paid_payment_copy_backup" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_id_trans_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4,
  "customer_id" int4,
  "virtual_account" varchar(255) COLLATE "pg_catalog"."default",
  "nominal" numeric(32,2),
  "invoice" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date DEFAULT now(),
  "tanggal_full" timestamp(6),
  "trans_type" varchar(255) COLLATE "pg_catalog"."default",
  "rekon_status" bool DEFAULT false,
  "name" varchar(255) COLLATE "pg_catalog"."default",
  "deskripsi" text COLLATE "pg_catalog"."default",
  "wrong_va" bool DEFAULT false,
  "bank_type" int2 DEFAULT 1,
  "rek_type" varchar(2) COLLATE "pg_catalog"."default" DEFAULT 'va'::character varying
)
;
COMMENT ON COLUMN "public"."trans_sales_order_paid_payment_copy_backup"."bank_type" IS '1. BTN VA, 2. Mandiri, 3.BTN RNI';
COMMENT ON COLUMN "public"."trans_sales_order_paid_payment_copy_backup"."rek_type" IS 'VA - Virutal Account / RA- regular account';

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_integration
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_integration";
CREATE TABLE "public"."trans_sales_order_paid_payment_integration" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_id_trans_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4,
  "customer_id" int4,
  "virtual_account" varchar(255) COLLATE "pg_catalog"."default",
  "nominal" numeric(32,2),
  "invoice" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date DEFAULT now(),
  "trans_type" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal_full" timestamp(6),
  "name" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_integration_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_integration_copy1";
CREATE TABLE "public"."trans_sales_order_paid_payment_integration_copy1" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_id_trans_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4,
  "customer_id" int4,
  "virtual_account" varchar(255) COLLATE "pg_catalog"."default",
  "nominal" numeric(32,2),
  "invoice" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date DEFAULT now(),
  "trans_type" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal_full" timestamp(6),
  "name" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_integration_dev
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_integration_dev";
CREATE TABLE "public"."trans_sales_order_paid_payment_integration_dev" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_id_trans_seq'::regclass),
  "company_id" int4,
  "cabang_id" int4,
  "customer_id" int4,
  "virtual_account" varchar(255) COLLATE "pg_catalog"."default",
  "nominal" numeric(32,2),
  "invoice" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal" date DEFAULT now(),
  "trans_type" varchar(255) COLLATE "pg_catalog"."default",
  "tanggal_full" timestamp(6),
  "name" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_inv
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_inv";
CREATE TABLE "public"."trans_sales_order_paid_payment_inv" (
  "id_" int8 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_inv_id__seq'::regclass),
  "id_trans_payment" int8,
  "invoice_id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "payment_nominal" numeric,
  "table_payment" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_inv_copy1
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_inv_copy1";
CREATE TABLE "public"."trans_sales_order_paid_payment_inv_copy1" (
  "id_" int8 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_inv_id__seq'::regclass),
  "id_trans_payment" int8,
  "invoice_id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "payment_nominal" numeric,
  "table_payment" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_inv_copy2
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_inv_copy2";
CREATE TABLE "public"."trans_sales_order_paid_payment_inv_copy2" (
  "id_" int8 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_inv_id__seq'::regclass),
  "id_trans_payment" int8,
  "invoice_id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "payment_nominal" numeric,
  "table_payment" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_inv_copy3
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_inv_copy3";
CREATE TABLE "public"."trans_sales_order_paid_payment_inv_copy3" (
  "id_" int8 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_inv_id__seq'::regclass),
  "id_trans_payment" int8,
  "invoice_id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "payment_nominal" numeric,
  "table_payment" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_inv_copy4
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_inv_copy4";
CREATE TABLE "public"."trans_sales_order_paid_payment_inv_copy4" (
  "id_" int8 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_inv_id__seq'::regclass),
  "id_trans_payment" int8,
  "invoice_id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "payment_nominal" numeric,
  "table_payment" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_inv_copy5
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_inv_copy5";
CREATE TABLE "public"."trans_sales_order_paid_payment_inv_copy5" (
  "id_" int8 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_inv_id__seq'::regclass),
  "id_trans_payment" int8,
  "invoice_id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "payment_nominal" numeric,
  "table_payment" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_inv_copy6
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_inv_copy6";
CREATE TABLE "public"."trans_sales_order_paid_payment_inv_copy6" (
  "id_" int8 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_inv_id__seq'::regclass),
  "id_trans_payment" int8,
  "invoice_id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "payment_nominal" numeric,
  "table_payment" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_inv_copy7
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_inv_copy7";
CREATE TABLE "public"."trans_sales_order_paid_payment_inv_copy7" (
  "id_" int8 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_inv_id__seq'::regclass),
  "id_trans_payment" int8,
  "invoice_id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "payment_nominal" numeric,
  "table_payment" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_inv_copy_backup
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_inv_copy_backup";
CREATE TABLE "public"."trans_sales_order_paid_payment_inv_copy_backup" (
  "id_" int8 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_inv_id__seq'::regclass),
  "id_trans_payment" int8,
  "invoice_id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "payment_nominal" numeric,
  "table_payment" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_order_paid_payment_inv_obsolete
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_order_paid_payment_inv_obsolete";
CREATE TABLE "public"."trans_sales_order_paid_payment_inv_obsolete" (
  "id_" int8 NOT NULL DEFAULT nextval('trans_sales_order_paid_payment_inv_id__seq'::regclass),
  "id_trans_payment" int8,
  "invoice_id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "payment_nominal" numeric,
  "table_payment" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for trans_sales_recap_detail
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_recap_detail";
CREATE TABLE "public"."trans_sales_recap_detail" (
  "id" int4 NOT NULL DEFAULT nextval('trans_sales_recap_detail_id_seq'::regclass),
  "id_header" int4,
  "invoice_number" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now()
)
;

-- ----------------------------
-- Table structure for trans_sales_recap_header
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_recap_header";
CREATE TABLE "public"."trans_sales_recap_header" (
  "id" int4 NOT NULL DEFAULT nextval('trans_sales_recap_header_id_seq'::regclass),
  "tanggal" timestamp(6),
  "status_release" bool DEFAULT false,
  "number_report" varchar(255) COLLATE "pg_catalog"."default",
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "bulan" int4,
  "tahun" int4,
  "no_urut" int4,
  "tanggal_start" date,
  "tanggal_end" date
)
;

-- ----------------------------
-- Table structure for trans_sales_recap_inventory_detail
-- ----------------------------
DROP TABLE IF EXISTS "public"."trans_sales_recap_inventory_detail";
CREATE TABLE "public"."trans_sales_recap_inventory_detail" (
  "id_trans" int4 NOT NULL DEFAULT nextval('trans_inventory_detail_id_trans_seq'::regclass),
  "produk_id" int4,
  "company_id" int4,
  "cabang_id" int4,
  "qty" int4,
  "harga_satuan" numeric(32,2),
  "harga_total" numeric(32,2),
  "updateindb" timestamp(6) DEFAULT now(),
  "userupdate" varchar(255) COLLATE "pg_catalog"."default",
  "id_sales_report" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Table structure for update_harga
-- ----------------------------
DROP TABLE IF EXISTS "public"."update_harga";
CREATE TABLE "public"."update_harga" (
  "id_increment" int4,
  "id_trans" varchar(255) COLLATE "pg_catalog"."default",
  "produk_id" int4,
  "old_harga_satuan" numeric(32,2),
  "new_harga_satuan" numeric(32,2),
  "old_harga_total" numeric(32,2),
  "new_harga_total" numeric(32,2)
)
;

-- ----------------------------
-- Table structure for va_number_record
-- ----------------------------
DROP TABLE IF EXISTS "public"."va_number_record";
CREATE TABLE "public"."va_number_record" (
  "va_number" varchar(255) COLLATE "pg_catalog"."default" NOT NULL
)
;

-- ----------------------------
-- Table structure for version_apps
-- ----------------------------
DROP TABLE IF EXISTS "public"."version_apps";
CREATE TABLE "public"."version_apps" (
  "version_id" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "status_release" bool DEFAULT false,
  "time_release" timestamp(6),
  "default_" bool,
  "deskripsi" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Function structure for reconsile_inv
-- ----------------------------
DROP FUNCTION IF EXISTS "public"."reconsile_inv"();
CREATE OR REPLACE FUNCTION "public"."reconsile_inv"()
  RETURNS "pg_catalog"."trigger" AS $BODY$
	DECLARE
		data_inv RECORD;
		outstanding FLOAT;
		outstanding_temp FLOAT;
		counter INTEGER := 0;
	BEGIN
-- 	trans_sales_order_paid_payment
-- rekon_status

-- 	trans_inventory_subsidiary_invoice
-- 		amount_total_outstanding
-- 		complete_payment

/*
ambil dari trans_inventory_subsidiary_invoice yang outstanding based on va
*/
for data_inv in
select a.*, b.account_va from trans_inventory_subsidiary_invoice a
        left join master_customer b on a.customer_id = b.id_customer
        where complete_payment='f' and b.account_va =NEW.virtual_account and a.tanggal_invoice<=NEW.tanggal ORDER BY a.tanggal_invoice asc
LOOP

counter := counter + 1;
    
IF counter = 1 THEN

-- RAISE NOTICE 'Trigger reconsile_inv dijalankan! VA: %', NEW.virtual_account;
outstanding = NEW.nominal-data_inv.amount_total_outstanding;
-- 	
	IF outstanding < 0 THEN
	
		 RAISE NOTICE 'part: %', 1;
	 outstanding = data_inv.amount_total_outstanding-NEW.nominal;
	 IF outstanding > 2500 then 
	 
		 RAISE NOTICE 'part: %', 2;
		 UPDATE trans_inventory_subsidiary_invoice SET amount_total_outstanding=outstanding, payment_last_updated=NEW.tanggal WHERE id_trans=data_inv.id_trans;
		 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
		 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, NEW.nominal, 'trans_inventory_subsidiary_invoice');
		 EXIT;
	 ELSE
	 
		 RAISE NOTICE 'part: %', 3;
			UPDATE trans_inventory_subsidiary_invoice SET amount_total_outstanding=0, complete_payment=TRUE, payment_last_updated=NEW.tanggal WHERE id_trans=data_inv.id_trans;
			update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
			INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding,'trans_inventory_subsidiary_invoice');
			EXIT;
	 END IF;
	 
	ELSE
	
		 RAISE NOTICE 'part: %', 4;
--    RAISE NOTICE 'Trigger reconsile_inv dijalankan! VA: %', NEW.virtual_account;
	 UPDATE trans_inventory_subsidiary_invoice SET amount_total_outstanding=0, complete_payment=TRUE, payment_last_updated=NEW.tanggal WHERE id_trans=data_inv.id_trans;
	 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
	 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding,'trans_inventory_subsidiary_invoice');
-- 	 RAISE NOTICE 'INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal) values (%, %, %);', NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding;
	 IF outstanding =0 THEN exit; end if;
	END IF;


	-- 	IF exit_condition THEN
	-- 		EXIT; 
	-- 	END IF; 
-- 	END LOOP;
ELSE

		 RAISE NOTICE 'part: %', 5;
	 outstanding_temp = outstanding;
	 outstanding = outstanding-data_inv.amount_total_outstanding;
-- 	 RAISE NOTICE 'Trigger 1 reconsile_inv dijalankan! VA: %', outstanding;
		IF outstanding < 0 THEN
		
		 RAISE NOTICE 'part: %', 6;
		 outstanding = data_inv.amount_total_outstanding-outstanding_temp;
-- 	   RAISE NOTICE 'Trigger 2 reconsile_inv dijalankan! VA: %', outstanding;
		 UPDATE trans_inventory_subsidiary_invoice SET amount_total_outstanding=outstanding, payment_last_updated=NEW.tanggal  WHERE id_trans=data_inv.id_trans;
		 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
		 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans,outstanding_temp,'trans_inventory_subsidiary_invoice');
		 EXIT;
		 
		ELSE
		
		 RAISE NOTICE 'part: %', 7;
-- 	   RAISE NOTICE 'Trigger 3 reconsile_inv dijalankan! VA: %', outstanding;
		
		 UPDATE trans_inventory_subsidiary_invoice SET amount_total_outstanding=0, complete_payment=TRUE,payment_last_updated=NEW.tanggal  WHERE id_trans=data_inv.id_trans;
		 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
		 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding, 'trans_inventory_subsidiary_invoice');
		 
		 IF outstanding =0 THEN exit; end if;
		END IF;
end IF;
END LOOP;


------for pre-paidment

for data_inv in
select a.*, b.account_va from trans_inventory_subsidiary_invoice_pre_payment a
        left join master_customer b on a.customer_id = b.id_customer
        where complete_payment='f' and b.account_va =NEW.virtual_account and a.tanggal_invoice<=NEW.tanggal ORDER BY a.tanggal_invoice asc
LOOP

counter := counter + 1;
    
IF counter = 1 THEN

-- RAISE NOTICE 'Trigger reconsile_inv dijalankan! VA: %', NEW.virtual_account;
outstanding = NEW.nominal-data_inv.amount_total_outstanding;
-- 	
	IF outstanding < 0 THEN
	
	 outstanding = data_inv.amount_total_outstanding-NEW.nominal;
	 IF outstanding > 2500 then 
	 
		 RAISE NOTICE 'part: %', 1;
		 UPDATE trans_inventory_subsidiary_invoice_pre_payment SET amount_total_outstanding=outstanding, payment_last_updated=NEW.tanggal WHERE id_trans=data_inv.id_trans;
		 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
		 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, NEW.nominal, 'trans_inventory_subsidiary_invoice_pre_payment');
		 EXIT;
	 ELSE
	 
		 RAISE NOTICE 'part: %', 2;
			UPDATE trans_inventory_subsidiary_invoice_pre_payment SET amount_total_outstanding=0, complete_payment=TRUE, payment_last_updated=NEW.tanggal WHERE id_trans=data_inv.id_trans;
			update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
			INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding,'trans_inventory_subsidiary_invoice_pre_payment');
			EXIT;
	 END IF;
	 
	ELSE
	
		 RAISE NOTICE 'part: %', 3;
--    RAISE NOTICE 'Trigger reconsile_inv dijalankan! VA: %', NEW.virtual_account;
	 UPDATE trans_inventory_subsidiary_invoice_pre_payment SET amount_total_outstanding=0, complete_payment=TRUE, payment_last_updated=NEW.tanggal WHERE id_trans=data_inv.id_trans;
	 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
	 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding, 'trans_inventory_subsidiary_invoice_pre_payment');
-- 	 RAISE NOTICE 'INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal) values (%, %, %);', NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding;
	 IF outstanding =0 THEN exit; end if;
	END IF;

ELSE

		 RAISE NOTICE 'part: %', 4;
	 outstanding_temp = outstanding;
	 outstanding = outstanding-data_inv.amount_total_outstanding;
-- 	 RAISE NOTICE 'Trigger 1 reconsile_inv dijalankan! VA: %', outstanding;
		IF outstanding < 0 THEN
		 RAISE NOTICE 'part: %', 5;
		 outstanding = data_inv.amount_total_outstanding-outstanding_temp;
-- 	   RAISE NOTICE 'Trigger 2 reconsile_inv dijalankan! VA: %', outstanding;
		 UPDATE trans_inventory_subsidiary_invoice_pre_payment SET amount_total_outstanding=outstanding, payment_last_updated=NEW.tanggal  WHERE id_trans=data_inv.id_trans;
		 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
		 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans,outstanding_temp,'trans_inventory_subsidiary_invoice_pre_payment');
		 EXIT;
		 
		ELSE
		 RAISE NOTICE 'part: %', 6;
-- 	   RAISE NOTICE 'Trigger 3 reconsile_inv dijalankan! VA: %', outstanding;
		
		 UPDATE trans_inventory_subsidiary_invoice_pre_payment SET amount_total_outstanding=0, complete_payment=TRUE,payment_last_updated=NEW.tanggal  WHERE id_trans=data_inv.id_trans;
		 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
		 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding,'trans_inventory_subsidiary_invoice_pre_payment');
		 
		 IF outstanding =0 THEN exit; end if;
		END IF;
end IF;
END LOOP;
		
	
	
	RETURN NEW;

END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- ----------------------------
-- Function structure for reconsile_inv_copy1
-- ----------------------------
DROP FUNCTION IF EXISTS "public"."reconsile_inv_copy1"();
CREATE OR REPLACE FUNCTION "public"."reconsile_inv_copy1"()
  RETURNS "pg_catalog"."trigger" AS $BODY$
	DECLARE
		data_inv RECORD;
		outstanding FLOAT;
		outstanding_temp FLOAT;
		counter INTEGER := 0;
	BEGIN
-- 	trans_sales_order_paid_payment
-- rekon_status

-- 	trans_inventory_subsidiary_invoice
-- 		amount_total_outstanding
-- 		complete_payment

/*
ambil dari trans_inventory_subsidiary_invoice yang outstanding based on va
*/
for data_inv in
select a.*, b.account_va from trans_inventory_subsidiary_invoice a
        left join master_customer b on a.customer_id = b.id_customer
        where complete_payment='f' and b.account_va =NEW.virtual_account and a.tanggal_invoice<=NEW.tanggal ORDER BY a.updateindb,a.id_trans asc
LOOP

counter := counter + 1;
    
IF counter = 1 THEN

-- RAISE NOTICE 'Trigger reconsile_inv dijalankan! VA: %', NEW.virtual_account;
outstanding = NEW.nominal-data_inv.amount_total_outstanding;
-- 	
	IF outstanding < 0 THEN
	
	 outstanding = data_inv.amount_total_outstanding-NEW.nominal;
	 IF outstanding > 2500 then 
		 UPDATE trans_inventory_subsidiary_invoice SET amount_total_outstanding=outstanding, payment_last_updated=NEW.tanggal WHERE id_trans=data_inv.id_trans;
		 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
		 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, NEW.nominal, 'trans_inventory_subsidiary_invoice');
		 EXIT;
	 ELSE
			UPDATE trans_inventory_subsidiary_invoice SET amount_total_outstanding=0, complete_payment=TRUE, payment_last_updated=NEW.tanggal WHERE id_trans=data_inv.id_trans;
			update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
			INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding,'trans_inventory_subsidiary_invoice');
			EXIT;
	 END IF;
	 
	ELSE
	
--    RAISE NOTICE 'Trigger reconsile_inv dijalankan! VA: %', NEW.virtual_account;
	 UPDATE trans_inventory_subsidiary_invoice SET amount_total_outstanding=0, complete_payment=TRUE, payment_last_updated=NEW.tanggal WHERE id_trans=data_inv.id_trans;
	 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
	 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding,'trans_inventory_subsidiary_invoice');
-- 	 RAISE NOTICE 'INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal) values (%, %, %);', NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding;
	 IF outstanding =0 THEN exit; end if;
	END IF;


	-- 	IF exit_condition THEN
	-- 		EXIT; 
	-- 	END IF; 
-- 	END LOOP;
ELSE

	 outstanding_temp = outstanding;
	 outstanding = outstanding-data_inv.amount_total_outstanding;
-- 	 RAISE NOTICE 'Trigger 1 reconsile_inv dijalankan! VA: %', outstanding;
		IF outstanding < 0 THEN
		
		 outstanding = data_inv.amount_total_outstanding-outstanding_temp;
-- 	   RAISE NOTICE 'Trigger 2 reconsile_inv dijalankan! VA: %', outstanding;
		 UPDATE trans_inventory_subsidiary_invoice SET amount_total_outstanding=outstanding, payment_last_updated=NEW.tanggal  WHERE id_trans=data_inv.id_trans;
		 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
		 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans,outstanding_temp,'trans_inventory_subsidiary_invoice');
		 EXIT;
		 
		ELSE
-- 	   RAISE NOTICE 'Trigger 3 reconsile_inv dijalankan! VA: %', outstanding;
		
		 UPDATE trans_inventory_subsidiary_invoice SET amount_total_outstanding=0, complete_payment=TRUE,payment_last_updated=NEW.tanggal  WHERE id_trans=data_inv.id_trans;
		 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
		 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding, 'trans_inventory_subsidiary_invoice');
		 
		 IF outstanding =0 THEN exit; end if;
		END IF;
end IF;
END LOOP;


------for pre-paidment

for data_inv in
select a.*, b.account_va from trans_inventory_subsidiary_invoice_pre_payment a
        left join master_customer b on a.customer_id = b.id_customer
        where complete_payment='f' and b.account_va =NEW.virtual_account  and a.tanggal_invoice<=NEW.tanggal ORDER BY a.updateindb,a.id_trans asc
LOOP

counter := counter + 1;
    
IF counter = 1 THEN

-- RAISE NOTICE 'Trigger reconsile_inv dijalankan! VA: %', NEW.virtual_account;
outstanding = NEW.nominal-data_inv.amount_total_outstanding;
-- 	
	IF outstanding < 0 THEN
	
	 outstanding = data_inv.amount_total_outstanding-NEW.nominal;
	 IF outstanding > 2500 then 
		 UPDATE trans_inventory_subsidiary_invoice_pre_payment SET amount_total_outstanding=outstanding, payment_last_updated=NEW.tanggal WHERE id_trans=data_inv.id_trans;
		 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
		 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, NEW.nominal, 'trans_inventory_subsidiary_invoice_pre_payment');
		 EXIT;
	 ELSE
			UPDATE trans_inventory_subsidiary_invoice_pre_payment SET amount_total_outstanding=0, complete_payment=TRUE, payment_last_updated=NEW.tanggal WHERE id_trans=data_inv.id_trans;
			update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
			INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding,'trans_inventory_subsidiary_invoice_pre_payment');
			EXIT;
	 END IF;
	 
	ELSE
	
--    RAISE NOTICE 'Trigger reconsile_inv dijalankan! VA: %', NEW.virtual_account;
	 UPDATE trans_inventory_subsidiary_invoice_pre_payment SET amount_total_outstanding=0, complete_payment=TRUE, payment_last_updated=NEW.tanggal WHERE id_trans=data_inv.id_trans;
	 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
	 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding, 'trans_inventory_subsidiary_invoice_pre_payment');
-- 	 RAISE NOTICE 'INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal) values (%, %, %);', NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding;
	 IF outstanding =0 THEN exit; end if;
	END IF;

ELSE

	 outstanding_temp = outstanding;
	 outstanding = outstanding-data_inv.amount_total_outstanding;
-- 	 RAISE NOTICE 'Trigger 1 reconsile_inv dijalankan! VA: %', outstanding;
		IF outstanding < 0 THEN
		
		 outstanding = data_inv.amount_total_outstanding-outstanding_temp;
-- 	   RAISE NOTICE 'Trigger 2 reconsile_inv dijalankan! VA: %', outstanding;
		 UPDATE trans_inventory_subsidiary_invoice_pre_payment SET amount_total_outstanding=outstanding, payment_last_updated=NEW.tanggal  WHERE id_trans=data_inv.id_trans;
		 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
		 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans,outstanding_temp,'trans_inventory_subsidiary_invoice_pre_payment');
		 EXIT;
		 
		ELSE
-- 	   RAISE NOTICE 'Trigger 3 reconsile_inv dijalankan! VA: %', outstanding;
		
		 UPDATE trans_inventory_subsidiary_invoice_pre_payment SET amount_total_outstanding=0, complete_payment=TRUE,payment_last_updated=NEW.tanggal  WHERE id_trans=data_inv.id_trans;
		 update trans_sales_order_paid_payment set rekon_status= TRUE WHERE id_trans=NEW.id_trans;
		 INSERT into trans_sales_order_paid_payment_inv (id_trans_payment, invoice_id, payment_nominal, table_payment) values (NEW.id_trans, data_inv.id_trans, data_inv.amount_total_outstanding,'trans_inventory_subsidiary_invoice_pre_payment');
		 
		 IF outstanding =0 THEN exit; end if;
		END IF;
end IF;
END LOOP;
		
	
	
	RETURN NEW;

END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- ----------------------------
-- Function structure for uuid_generate_v1
-- ----------------------------
DROP FUNCTION IF EXISTS "public"."uuid_generate_v1"();
CREATE OR REPLACE FUNCTION "public"."uuid_generate_v1"()
  RETURNS "pg_catalog"."uuid" AS '$libdir/uuid-ossp', 'uuid_generate_v1'
  LANGUAGE c VOLATILE STRICT
  COST 1;

-- ----------------------------
-- Function structure for uuid_generate_v1mc
-- ----------------------------
DROP FUNCTION IF EXISTS "public"."uuid_generate_v1mc"();
CREATE OR REPLACE FUNCTION "public"."uuid_generate_v1mc"()
  RETURNS "pg_catalog"."uuid" AS '$libdir/uuid-ossp', 'uuid_generate_v1mc'
  LANGUAGE c VOLATILE STRICT
  COST 1;

-- ----------------------------
-- Function structure for uuid_generate_v3
-- ----------------------------
DROP FUNCTION IF EXISTS "public"."uuid_generate_v3"("namespace" uuid, "name" text);
CREATE OR REPLACE FUNCTION "public"."uuid_generate_v3"("namespace" uuid, "name" text)
  RETURNS "pg_catalog"."uuid" AS '$libdir/uuid-ossp', 'uuid_generate_v3'
  LANGUAGE c IMMUTABLE STRICT
  COST 1;

-- ----------------------------
-- Function structure for uuid_generate_v4
-- ----------------------------
DROP FUNCTION IF EXISTS "public"."uuid_generate_v4"();
CREATE OR REPLACE FUNCTION "public"."uuid_generate_v4"()
  RETURNS "pg_catalog"."uuid" AS '$libdir/uuid-ossp', 'uuid_generate_v4'
  LANGUAGE c VOLATILE STRICT
  COST 1;

-- ----------------------------
-- Function structure for uuid_generate_v5
-- ----------------------------
DROP FUNCTION IF EXISTS "public"."uuid_generate_v5"("namespace" uuid, "name" text);
CREATE OR REPLACE FUNCTION "public"."uuid_generate_v5"("namespace" uuid, "name" text)
  RETURNS "pg_catalog"."uuid" AS '$libdir/uuid-ossp', 'uuid_generate_v5'
  LANGUAGE c IMMUTABLE STRICT
  COST 1;

-- ----------------------------
-- Function structure for uuid_nil
-- ----------------------------
DROP FUNCTION IF EXISTS "public"."uuid_nil"();
CREATE OR REPLACE FUNCTION "public"."uuid_nil"()
  RETURNS "pg_catalog"."uuid" AS '$libdir/uuid-ossp', 'uuid_nil'
  LANGUAGE c IMMUTABLE STRICT
  COST 1;

-- ----------------------------
-- Function structure for uuid_ns_dns
-- ----------------------------
DROP FUNCTION IF EXISTS "public"."uuid_ns_dns"();
CREATE OR REPLACE FUNCTION "public"."uuid_ns_dns"()
  RETURNS "pg_catalog"."uuid" AS '$libdir/uuid-ossp', 'uuid_ns_dns'
  LANGUAGE c IMMUTABLE STRICT
  COST 1;

-- ----------------------------
-- Function structure for uuid_ns_oid
-- ----------------------------
DROP FUNCTION IF EXISTS "public"."uuid_ns_oid"();
CREATE OR REPLACE FUNCTION "public"."uuid_ns_oid"()
  RETURNS "pg_catalog"."uuid" AS '$libdir/uuid-ossp', 'uuid_ns_oid'
  LANGUAGE c IMMUTABLE STRICT
  COST 1;

-- ----------------------------
-- Function structure for uuid_ns_url
-- ----------------------------
DROP FUNCTION IF EXISTS "public"."uuid_ns_url"();
CREATE OR REPLACE FUNCTION "public"."uuid_ns_url"()
  RETURNS "pg_catalog"."uuid" AS '$libdir/uuid-ossp', 'uuid_ns_url'
  LANGUAGE c IMMUTABLE STRICT
  COST 1;

-- ----------------------------
-- Function structure for uuid_ns_x500
-- ----------------------------
DROP FUNCTION IF EXISTS "public"."uuid_ns_x500"();
CREATE OR REPLACE FUNCTION "public"."uuid_ns_x500"()
  RETURNS "pg_catalog"."uuid" AS '$libdir/uuid-ossp', 'uuid_ns_x500'
  LANGUAGE c IMMUTABLE STRICT
  COST 1;

-- ----------------------------
-- View structure for inv_detail_vs_generate_inv
-- ----------------------------
DROP VIEW IF EXISTS "public"."inv_detail_vs_generate_inv";
CREATE VIEW "public"."inv_detail_vs_generate_inv" AS  SELECT a.company_id,
    a.cabang_id,
    a.cabang_name,
    a.qty AS qty_inv,
    a.harga_satuan AS harga_satuan_inv,
    a.harga_total AS harga_total_inv,
    b.qty,
    b.harga_satuan,
    b.harga_total,
    a.qty - b.qty AS sel_qty,
    a.harga_satuan - b.harga_satuan AS hs_sel,
    a.harga_total - b.harga_total AS ht_sel
   FROM ( SELECT a_1.produk_id,
            a_1.company_id,
            d.cabang_name,
            a_1.cabang_id,
            a_1.qty_in,
            a_1.qty_out,
            a_1.qty_in - a_1.qty_out AS qty,
                CASE
                    WHEN (a_1.qty_in - a_1.qty_out) = 0 THEN 0::numeric
                    ELSE round((a_1.ht_in - a_1.ht_out) / (a_1.qty_in - a_1.qty_out)::numeric, 2)
                END AS harga_satuan,
            a_1.ht_in - a_1.ht_out AS harga_total
           FROM ( SELECT trans_inventory_detail_mutasi.produk_id,
                    trans_inventory_detail_mutasi.company_id,
                    trans_inventory_detail_mutasi.cabang_id,
                    sum(
                        CASE
                            WHEN trans_inventory_detail_mutasi.in_out::text = 'IN'::text THEN trans_inventory_detail_mutasi.qty
                            ELSE 0
                        END) AS qty_in,
                    sum(
                        CASE
                            WHEN trans_inventory_detail_mutasi.in_out::text = 'OUT'::text THEN trans_inventory_detail_mutasi.qty
                            ELSE 0
                        END) AS qty_out,
                    sum(
                        CASE
                            WHEN trans_inventory_detail_mutasi.in_out::text = 'IN'::text THEN trans_inventory_detail_mutasi.harga_total
                            ELSE 0::numeric
                        END) AS ht_in,
                    sum(
                        CASE
                            WHEN trans_inventory_detail_mutasi.in_out::text = 'OUT'::text THEN trans_inventory_detail_mutasi.harga_total
                            ELSE 0::numeric
                        END) AS ht_out
                   FROM trans_inventory_detail_mutasi
                  GROUP BY trans_inventory_detail_mutasi.produk_id, trans_inventory_detail_mutasi.company_id, trans_inventory_detail_mutasi.cabang_id) a_1
             LEFT JOIN master_company c ON a_1.company_id = c.id_company
             LEFT JOIN master_company_cabang d ON a_1.company_id = d.id_company AND d.id_cabang = a_1.cabang_id
          ORDER BY d.cabang_name, a_1.produk_id) a
     LEFT JOIN trans_inventory_detail b ON a.produk_id = b.produk_id AND a.company_id = b.company_id AND a.cabang_id = b.cabang_id;

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."files_retur_id_file_seq"
OWNED BY "public"."files_retur"."id_file";
SELECT setval('"public"."files_retur_id_file_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."files_upload_id_file_seq"
OWNED BY "public"."files_upload"."id_file";
SELECT setval('"public"."files_upload_id_file_seq"', 25512, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_approval_id_seq"
OWNED BY "public"."master_approval"."id";
SELECT setval('"public"."master_approval_id_seq"', 697, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_approval_type_id__seq"
OWNED BY "public"."master_approval_type"."id_";
SELECT setval('"public"."master_approval_type_id__seq"', 8, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_approval_type_id_type_seq"
OWNED BY "public"."master_approval_status"."id_status";
SELECT setval('"public"."master_approval_type_id_type_seq"', 4, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_biaya_admin_id_biaya_seq"
OWNED BY "public"."master_biaya_admin"."id_biaya";
SELECT setval('"public"."master_biaya_admin_id_biaya_seq"', 147, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_cabang_id_cabang_seq"
OWNED BY "public"."master_company_cabang"."id_cabang";
SELECT setval('"public"."master_cabang_id_cabang_seq"', 17, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_company_id_company_seq"
OWNED BY "public"."master_company"."id_company";
SELECT setval('"public"."master_company_id_company_seq"', 5, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"public"."master_customer_id_customer_seq"', 17, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_customer_id_increment_seq"
OWNED BY "public"."master_customer"."id_increment";
SELECT setval('"public"."master_customer_id_increment_seq"', 89234, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_jenis_pembayaran_id_pembayaran_seq"
OWNED BY "public"."master_jenis_pembayaran"."id_pembayaran";
SELECT setval('"public"."master_jenis_pembayaran_id_pembayaran_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_kategori_id_kategori_seq"
OWNED BY "public"."master_produk_kategori"."id_kategori";
SELECT setval('"public"."master_kategori_id_kategori_seq"', 4, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_minimum_qty_id_seq"
OWNED BY "public"."master_minimum_qty"."id";
SELECT setval('"public"."master_minimum_qty_id_seq"', 24, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_pajak_id_pajak_seq"
OWNED BY "public"."master_pajak"."id_pajak";
SELECT setval('"public"."master_pajak_id_pajak_seq"', 2, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_produk_id_produk_seq"
OWNED BY "public"."master_produk"."id_produk";
SELECT setval('"public"."master_produk_id_produk_seq"', 15, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_sales_price_id_price_seq"
OWNED BY "public"."master_sales_price"."id_price";
SELECT setval('"public"."master_sales_price_id_price_seq"', 540, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_template_download_id_seq"
OWNED BY "public"."master_template_download"."id";
SELECT setval('"public"."master_template_download_id_seq"', 2, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_uom_satuan_id_uom_satuan_seq"
OWNED BY "public"."master_produk_uom_satuan"."id_uom_satuan";
SELECT setval('"public"."master_uom_satuan_id_uom_satuan_seq"', 5, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_user_id_user_seq"
OWNED BY "public"."master_user"."id_user";
SELECT setval('"public"."master_user_id_user_seq"', 569, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."master_user_role_id_role_seq"
OWNED BY "public"."master_user_role"."id_role";
SELECT setval('"public"."master_user_role_id_role_seq"', 6, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_approval_detail_detail_id_seq"
OWNED BY "public"."trans_approval_detail"."detail_id";
SELECT setval('"public"."trans_approval_detail_detail_id_seq"', 2115, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_harga_produk_id_trans_seq"
OWNED BY "public"."trans_produk_harga_jual"."id_trans";
SELECT setval('"public"."trans_harga_produk_id_trans_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_detail_bad_stock_header_id_increment_seq"
OWNED BY "public"."trans_inventory_detail_bad_stock_header"."id_increment";
SELECT setval('"public"."trans_inventory_detail_bad_stock_header_id_increment_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_detail_bad_stock_id_increment_seq"
OWNED BY "public"."trans_inventory_detail_bad_stock"."id_increment";
SELECT setval('"public"."trans_inventory_detail_bad_stock_id_increment_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_detail_id_trans_seq"
OWNED BY "public"."trans_inventory_detail"."id_trans";
SELECT setval('"public"."trans_inventory_detail_id_trans_seq"', 22720, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_detail_mutasi_id_trans_seq"
OWNED BY "public"."trans_inventory_detail_mutasi"."id_trans";
SELECT setval('"public"."trans_inventory_detail_mutasi_id_trans_seq"', 30704, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_holding_delivery_preparation_header_id_seq"
OWNED BY "public"."trans_inventory_holding_delivery_preparation_header"."id";
SELECT setval('"public"."trans_inventory_holding_delivery_preparation_header_id_seq"', 2050, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_holding_delivery_preparation_id_increment_seq"
OWNED BY "public"."trans_inventory_holding_delivery_preparation"."id_increment";
SELECT setval('"public"."trans_inventory_holding_delivery_preparation_id_increment_seq"', 2058, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_holding_delivery_preparation_id_trans_seq"
OWNED BY "public"."trans_inventory_holding_delivery_preparation"."id_trans";
SELECT setval('"public"."trans_inventory_holding_delivery_preparation_id_trans_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_holding_submit_id_increment_seq"
OWNED BY "public"."trans_inventory_holding_submit"."id_increment";
SELECT setval('"public"."trans_inventory_holding_submit_id_increment_seq"', 553, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_holding_submit_id_trans_seq"
OWNED BY "public"."trans_inventory_holding_submit"."id_trans";
SELECT setval('"public"."trans_inventory_holding_submit_id_trans_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_holding_transfer_id_incement_seq"
OWNED BY "public"."trans_inventory_holding_transfer"."id_increment";
SELECT setval('"public"."trans_inventory_holding_transfer_id_incement_seq"', 621, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_holding_transfer_id_trans_seq"
OWNED BY "public"."trans_inventory_holding_transfer"."id_trans";
SELECT setval('"public"."trans_inventory_holding_transfer_id_trans_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_subsidiary_delivery_order_id_increment_seq"
OWNED BY "public"."trans_inventory_subsidiary_delivery_order"."id_increment";
SELECT setval('"public"."trans_inventory_subsidiary_delivery_order_id_increment_seq"', 21792, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_subsidiary_delivery_order_id_trans_seq"
OWNED BY "public"."trans_inventory_subsidiary_delivery_order"."id_trans";
SELECT setval('"public"."trans_inventory_subsidiary_delivery_order_id_trans_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_subsidiary_invoice_id_increment_seq"
OWNED BY "public"."trans_inventory_subsidiary_invoice"."id_increment";
SELECT setval('"public"."trans_inventory_subsidiary_invoice_id_increment_seq"', 23818, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_subsidiary_invoice_id_trans_seq"
OWNED BY "public"."trans_inventory_subsidiary_invoice"."id_trans";
SELECT setval('"public"."trans_inventory_subsidiary_invoice_id_trans_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_subsidiary_receipt_id_trans_seq"
OWNED BY "public"."trans_inventory_subsidiary_receipt_transfer"."id_trans";
SELECT setval('"public"."trans_inventory_subsidiary_receipt_id_trans_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_subsidiary_receipt_transfer_id_increment_seq"
OWNED BY "public"."trans_inventory_subsidiary_receipt_transfer"."id_increment";
SELECT setval('"public"."trans_inventory_subsidiary_receipt_transfer_id_increment_seq"', 636, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_subsidiary_retur_detail_id_detail_seq"
OWNED BY "public"."trans_inventory_subsidiary_retur_detail"."id_detail";
SELECT setval('"public"."trans_inventory_subsidiary_retur_detail_id_detail_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_subsidiary_retur_header_id_increment_seq"
OWNED BY "public"."trans_inventory_subsidiary_retur_header"."id_increment";
SELECT setval('"public"."trans_inventory_subsidiary_retur_header_id_increment_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_subsidiary_sales_order_header_id_seq"
OWNED BY "public"."trans_inventory_subsidiary_sales_order_header"."id";
SELECT setval('"public"."trans_inventory_subsidiary_sales_order_header_id_seq"', 23327, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_subsidiary_sales_order_id_increment_seq"
OWNED BY "public"."trans_inventory_subsidiary_sales_order"."id_increment";
SELECT setval('"public"."trans_inventory_subsidiary_sales_order_id_increment_seq"', 24538, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_inventory_subsidiary_sales_order_id_trans_seq"
OWNED BY "public"."trans_inventory_subsidiary_sales_order"."id_trans";
SELECT setval('"public"."trans_inventory_subsidiary_sales_order_id_trans_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_produk_harga_jual_id_increment_seq"
OWNED BY "public"."trans_produk_harga_jual"."id_increment";
SELECT setval('"public"."trans_produk_harga_jual_id_increment_seq"', 1, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_sales_order_paid_payment_id_trans_seq"
OWNED BY "public"."trans_sales_order_paid_payment"."id_trans";
SELECT setval('"public"."trans_sales_order_paid_payment_id_trans_seq"', 623571, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_sales_order_paid_payment_inv_id__seq"
OWNED BY "public"."trans_sales_order_paid_payment_inv_obsolete"."id_";
SELECT setval('"public"."trans_sales_order_paid_payment_inv_id__seq"', 32067, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_sales_recap_detail_id_seq"
OWNED BY "public"."trans_sales_recap_detail"."id";
SELECT setval('"public"."trans_sales_recap_detail_id_seq"', 58027, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "public"."trans_sales_recap_header_id_seq"
OWNED BY "public"."trans_sales_recap_header"."id";
SELECT setval('"public"."trans_sales_recap_header_id_seq"', 48, true);

-- ----------------------------
-- Primary Key structure for table master_customer
-- ----------------------------
ALTER TABLE "public"."master_customer" ADD CONSTRAINT "master_customer_pkey" PRIMARY KEY ("id_customer", "account_va");

-- ----------------------------
-- Primary Key structure for table master_customer_copy1
-- ----------------------------
ALTER TABLE "public"."master_customer_copy1" ADD CONSTRAINT "master_customer_copy1_pkey" PRIMARY KEY ("id_customer", "account_va");

-- ----------------------------
-- Primary Key structure for table master_customer_copy2
-- ----------------------------
ALTER TABLE "public"."master_customer_copy2" ADD CONSTRAINT "master_customer_copy2_pkey" PRIMARY KEY ("id_customer", "account_va");

-- ----------------------------
-- Primary Key structure for table master_kecamatan
-- ----------------------------
ALTER TABLE "public"."master_kecamatan" ADD CONSTRAINT "kecamatan_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Indexes structure for table master_kelurahan
-- ----------------------------
CREATE INDEX "_kec" ON "public"."master_kelurahan" USING btree (
  "kode_kec" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "_kel" ON "public"."master_kelurahan" USING btree (
  "kode_kel" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "_kota" ON "public"."master_kelurahan" USING btree (
  "kode_kotakab" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "_prov" ON "public"."master_kelurahan" USING btree (
  "kode_prov" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table master_kelurahan
-- ----------------------------
ALTER TABLE "public"."master_kelurahan" ADD CONSTRAINT "kelurahan_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Primary Key structure for table master_kotakab
-- ----------------------------
ALTER TABLE "public"."master_kotakab" ADD CONSTRAINT "kota_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Primary Key structure for table master_minimum_qty
-- ----------------------------
ALTER TABLE "public"."master_minimum_qty" ADD CONSTRAINT "master_minimum_qty_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Indexes structure for table master_produk
-- ----------------------------
CREATE INDEX "kategori" ON "public"."master_produk" USING btree (
  "kategori_produk" "pg_catalog"."int4_ops" ASC NULLS LAST
);
CREATE INDEX "uom" ON "public"."master_produk" USING btree (
  "uom_satuan" "pg_catalog"."int4_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table master_provinsi
-- ----------------------------
ALTER TABLE "public"."master_provinsi" ADD CONSTRAINT "provinsi_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Primary Key structure for table master_unit_geografis
-- ----------------------------
ALTER TABLE "public"."master_unit_geografis" ADD CONSTRAINT "master_unit_geografis_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Primary Key structure for table master_user
-- ----------------------------
ALTER TABLE "public"."master_user" ADD CONSTRAINT "master_user_pkey" PRIMARY KEY ("username");

-- ----------------------------
-- Primary Key structure for table master_user_role
-- ----------------------------
ALTER TABLE "public"."master_user_role" ADD CONSTRAINT "master_user_role_pkey" PRIMARY KEY ("id_role");

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast" ON "public"."trans_inventory_detail_mutasi" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy10
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy3" ON "public"."trans_inventory_detail_mutasi_copy10" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy11
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy4" ON "public"."trans_inventory_detail_mutasi_copy11" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy12
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy5" ON "public"."trans_inventory_detail_mutasi_copy12" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy13
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy6" ON "public"."trans_inventory_detail_mutasi_copy13" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy14
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy7" ON "public"."trans_inventory_detail_mutasi_copy14" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy15
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy8" ON "public"."trans_inventory_detail_mutasi_copy15" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy16
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy9" ON "public"."trans_inventory_detail_mutasi_copy16" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy17
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy10" ON "public"."trans_inventory_detail_mutasi_copy17" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy18
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy11" ON "public"."trans_inventory_detail_mutasi_copy18" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy19
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy12" ON "public"."trans_inventory_detail_mutasi_copy19" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy20
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy13" ON "public"."trans_inventory_detail_mutasi_copy20" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy21
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy14" ON "public"."trans_inventory_detail_mutasi_copy21" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy22
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy15" ON "public"."trans_inventory_detail_mutasi_copy22" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy8
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy1" ON "public"."trans_inventory_detail_mutasi_copy8" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy9
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy2" ON "public"."trans_inventory_detail_mutasi_copy9" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy_03_16_20206
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy16" ON "public"."trans_inventory_detail_mutasi_copy_03_16_20206" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_detail_mutasi_copy_03_16_20206_copy1
-- ----------------------------
CREATE INDEX "idx_mutasi_update_fast_copy16_copy1" ON "public"."trans_inventory_detail_mutasi_copy_03_16_20206_copy1" USING btree (
  "mutasi_type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "tanggal" "pg_catalog"."date_ops" ASC NULLS LAST,
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "id_references" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table trans_inventory_holding_delivery_preparation
-- ----------------------------
ALTER TABLE "public"."trans_inventory_holding_delivery_preparation" ADD CONSTRAINT "trans_inventory_holding_delivery_preparation_pkey1" PRIMARY KEY ("id_increment");

-- ----------------------------
-- Primary Key structure for table trans_inventory_holding_delivery_preparation_copy1
-- ----------------------------
ALTER TABLE "public"."trans_inventory_holding_delivery_preparation_copy1" ADD CONSTRAINT "trans_inventory_holding_delivery_preparation_copy1_pkey" PRIMARY KEY ("id_increment");

-- ----------------------------
-- Indexes structure for table trans_inventory_holding_delivery_preparation_header
-- ----------------------------
CREATE INDEX "trans_inventory_holding_delivery_prepa_id_trans_sales_order_idx" ON "public"."trans_inventory_holding_delivery_preparation_header" USING btree (
  "id_trans_sales_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "trans_inventory_holding_delivery_preparation_heade_id_trans_idx" ON "public"."trans_inventory_holding_delivery_preparation_header" USING btree (
  "id_trans" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table trans_inventory_holding_delivery_preparation_header
-- ----------------------------
ALTER TABLE "public"."trans_inventory_holding_delivery_preparation_header" ADD CONSTRAINT "trans_inventory_holding_delivery_preparation_pkey" PRIMARY KEY ("id", "id_trans");

-- ----------------------------
-- Primary Key structure for table trans_inventory_holding_delivery_preparation_header_copy1
-- ----------------------------
ALTER TABLE "public"."trans_inventory_holding_delivery_preparation_header_copy1" ADD CONSTRAINT "trans_inventory_holding_delivery_preparation_header_copy1_pkey" PRIMARY KEY ("id", "id_trans");

-- ----------------------------
-- Primary Key structure for table trans_inventory_holding_delivery_preparation_header_copy2
-- ----------------------------
ALTER TABLE "public"."trans_inventory_holding_delivery_preparation_header_copy2" ADD CONSTRAINT "trans_inventory_holding_delivery_preparation_header_copy2_pkey" PRIMARY KEY ("id", "id_trans");

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_invoice
-- ----------------------------
CREATE INDEX "customer_id" ON "public"."trans_inventory_subsidiary_invoice" USING btree (
  "customer_id" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_delivery_order" ON "public"."trans_inventory_subsidiary_invoice" USING btree (
  "id_trans_delivery_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_sales_order" ON "public"."trans_inventory_subsidiary_invoice" USING btree (
  "id_trans_sales_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table trans_inventory_subsidiary_invoice
-- ----------------------------
ALTER TABLE "public"."trans_inventory_subsidiary_invoice" ADD CONSTRAINT "trans_inventory_subsidiary_invoice_pkey" PRIMARY KEY ("id_trans");

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_invoice_before_perbaikan
-- ----------------------------
CREATE INDEX "customer_id_copy8" ON "public"."trans_inventory_subsidiary_invoice_before_perbaikan" USING btree (
  "customer_id" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_delivery_order_copy8" ON "public"."trans_inventory_subsidiary_invoice_before_perbaikan" USING btree (
  "id_trans_delivery_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_sales_order_copy8" ON "public"."trans_inventory_subsidiary_invoice_before_perbaikan" USING btree (
  "id_trans_sales_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table trans_inventory_subsidiary_invoice_before_perbaikan
-- ----------------------------
ALTER TABLE "public"."trans_inventory_subsidiary_invoice_before_perbaikan" ADD CONSTRAINT "trans_inventory_subsidiary_invoice_copy6_pkey" PRIMARY KEY ("id_trans");

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_invoice_copy1
-- ----------------------------
CREATE INDEX "customer_id_copy2" ON "public"."trans_inventory_subsidiary_invoice_copy1" USING btree (
  "customer_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_delivery_order_copy2" ON "public"."trans_inventory_subsidiary_invoice_copy1" USING btree (
  "id_trans_delivery_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_sales_order_copy2" ON "public"."trans_inventory_subsidiary_invoice_copy1" USING btree (
  "id_trans_sales_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table trans_inventory_subsidiary_invoice_copy1
-- ----------------------------
ALTER TABLE "public"."trans_inventory_subsidiary_invoice_copy1" ADD CONSTRAINT "trans_inventory_subsidiary_invoice_copy1_pkey1" PRIMARY KEY ("id_trans");

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_invoice_copy2
-- ----------------------------
CREATE INDEX "customer_id_copy3" ON "public"."trans_inventory_subsidiary_invoice_copy2" USING btree (
  "customer_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_delivery_order_copy3" ON "public"."trans_inventory_subsidiary_invoice_copy2" USING btree (
  "id_trans_delivery_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_sales_order_copy3" ON "public"."trans_inventory_subsidiary_invoice_copy2" USING btree (
  "id_trans_sales_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table trans_inventory_subsidiary_invoice_copy2
-- ----------------------------
ALTER TABLE "public"."trans_inventory_subsidiary_invoice_copy2" ADD CONSTRAINT "trans_inventory_subsidiary_invoice_copy2_pkey" PRIMARY KEY ("id_trans");

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_invoice_copy3
-- ----------------------------
CREATE INDEX "customer_id_copy4" ON "public"."trans_inventory_subsidiary_invoice_copy3" USING btree (
  "customer_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_delivery_order_copy4" ON "public"."trans_inventory_subsidiary_invoice_copy3" USING btree (
  "id_trans_delivery_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_sales_order_copy4" ON "public"."trans_inventory_subsidiary_invoice_copy3" USING btree (
  "id_trans_sales_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table trans_inventory_subsidiary_invoice_copy3
-- ----------------------------
ALTER TABLE "public"."trans_inventory_subsidiary_invoice_copy3" ADD CONSTRAINT "trans_inventory_subsidiary_invoice_copy3_pkey" PRIMARY KEY ("id_trans");

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_invoice_copy4
-- ----------------------------
CREATE INDEX "customer_id_copy5" ON "public"."trans_inventory_subsidiary_invoice_copy4" USING btree (
  "customer_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_delivery_order_copy5" ON "public"."trans_inventory_subsidiary_invoice_copy4" USING btree (
  "id_trans_delivery_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_sales_order_copy5" ON "public"."trans_inventory_subsidiary_invoice_copy4" USING btree (
  "id_trans_sales_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table trans_inventory_subsidiary_invoice_copy4
-- ----------------------------
ALTER TABLE "public"."trans_inventory_subsidiary_invoice_copy4" ADD CONSTRAINT "trans_inventory_subsidiary_invoice_copy4_pkey" PRIMARY KEY ("id_trans");

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_invoice_copy5
-- ----------------------------
CREATE INDEX "customer_id_copy7" ON "public"."trans_inventory_subsidiary_invoice_copy5" USING btree (
  "customer_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_delivery_order_copy7" ON "public"."trans_inventory_subsidiary_invoice_copy5" USING btree (
  "id_trans_delivery_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_sales_order_copy7" ON "public"."trans_inventory_subsidiary_invoice_copy5" USING btree (
  "id_trans_sales_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table trans_inventory_subsidiary_invoice_copy5
-- ----------------------------
ALTER TABLE "public"."trans_inventory_subsidiary_invoice_copy5" ADD CONSTRAINT "trans_inventory_subsidiary_invoice_copy5_pkey1" PRIMARY KEY ("id_trans");

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_invoice_deleted
-- ----------------------------
CREATE INDEX "customer_id_copy6" ON "public"."trans_inventory_subsidiary_invoice_deleted" USING btree (
  "customer_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_delivery_order_copy6" ON "public"."trans_inventory_subsidiary_invoice_deleted" USING btree (
  "id_trans_delivery_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_sales_order_copy6" ON "public"."trans_inventory_subsidiary_invoice_deleted" USING btree (
  "id_trans_sales_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table trans_inventory_subsidiary_invoice_deleted
-- ----------------------------
ALTER TABLE "public"."trans_inventory_subsidiary_invoice_deleted" ADD CONSTRAINT "trans_inventory_subsidiary_invoice_copy5_pkey" PRIMARY KEY ("id_trans");

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_invoice_pre_payment
-- ----------------------------
CREATE INDEX "customer_id_copy1" ON "public"."trans_inventory_subsidiary_invoice_pre_payment" USING btree (
  "customer_id" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_delivery_order_copy1" ON "public"."trans_inventory_subsidiary_invoice_pre_payment" USING btree (
  "id_trans_delivery_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_sales_order_copy1" ON "public"."trans_inventory_subsidiary_invoice_pre_payment" USING btree (
  "id_trans_sales_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table trans_inventory_subsidiary_invoice_pre_payment
-- ----------------------------
ALTER TABLE "public"."trans_inventory_subsidiary_invoice_pre_payment" ADD CONSTRAINT "trans_inventory_subsidiary_invoice_copy1_pkey" PRIMARY KEY ("id_trans");

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_invoice_pre_payment_copy1
-- ----------------------------
CREATE INDEX "customer_id_copy1_copy1" ON "public"."trans_inventory_subsidiary_invoice_pre_payment_copy1" USING btree (
  "customer_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_delivery_order_copy1_copy1" ON "public"."trans_inventory_subsidiary_invoice_pre_payment_copy1" USING btree (
  "id_trans_delivery_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "id_trans_sales_order_copy1_copy1" ON "public"."trans_inventory_subsidiary_invoice_pre_payment_copy1" USING btree (
  "id_trans_sales_order" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table trans_inventory_subsidiary_invoice_pre_payment_copy1
-- ----------------------------
ALTER TABLE "public"."trans_inventory_subsidiary_invoice_pre_payment_copy1" ADD CONSTRAINT "trans_inventory_subsidiary_invoice_pre_payment_copy1_pkey" PRIMARY KEY ("id_trans");

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_sales_order
-- ----------------------------
CREATE INDEX "cabang_id" ON "public"."trans_inventory_subsidiary_sales_order" USING btree (
  "cabang_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);
CREATE INDEX "company_id" ON "public"."trans_inventory_subsidiary_sales_order" USING btree (
  "company_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);
CREATE INDEX "produk_id" ON "public"."trans_inventory_subsidiary_sales_order" USING btree (
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_sales_order_copy1
-- ----------------------------
CREATE INDEX "cabang_id_copy1" ON "public"."trans_inventory_subsidiary_sales_order_copy1" USING btree (
  "cabang_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);
CREATE INDEX "company_id_copy1" ON "public"."trans_inventory_subsidiary_sales_order_copy1" USING btree (
  "company_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);
CREATE INDEX "produk_id_copy1" ON "public"."trans_inventory_subsidiary_sales_order_copy1" USING btree (
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_sales_order_copy2
-- ----------------------------
CREATE INDEX "cabang_id_copy2" ON "public"."trans_inventory_subsidiary_sales_order_copy2" USING btree (
  "cabang_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);
CREATE INDEX "company_id_copy2" ON "public"."trans_inventory_subsidiary_sales_order_copy2" USING btree (
  "company_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);
CREATE INDEX "produk_id_copy2" ON "public"."trans_inventory_subsidiary_sales_order_copy2" USING btree (
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_sales_order_copy3
-- ----------------------------
CREATE INDEX "cabang_id_copy3" ON "public"."trans_inventory_subsidiary_sales_order_copy3" USING btree (
  "cabang_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);
CREATE INDEX "company_id_copy3" ON "public"."trans_inventory_subsidiary_sales_order_copy3" USING btree (
  "company_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);
CREATE INDEX "produk_id_copy3" ON "public"."trans_inventory_subsidiary_sales_order_copy3" USING btree (
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_sales_order_copy4
-- ----------------------------
CREATE INDEX "cabang_id_copy4" ON "public"."trans_inventory_subsidiary_sales_order_copy4" USING btree (
  "cabang_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);
CREATE INDEX "company_id_copy4" ON "public"."trans_inventory_subsidiary_sales_order_copy4" USING btree (
  "company_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);
CREATE INDEX "produk_id_copy4" ON "public"."trans_inventory_subsidiary_sales_order_copy4" USING btree (
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table trans_inventory_subsidiary_sales_order_copy_before_perbaikan
-- ----------------------------
CREATE INDEX "cabang_id_copy5" ON "public"."trans_inventory_subsidiary_sales_order_copy_before_perbaikan" USING btree (
  "cabang_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);
CREATE INDEX "company_id_copy5" ON "public"."trans_inventory_subsidiary_sales_order_copy_before_perbaikan" USING btree (
  "company_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);
CREATE INDEX "produk_id_copy5" ON "public"."trans_inventory_subsidiary_sales_order_copy_before_perbaikan" USING btree (
  "produk_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table trans_inventory_subsidiary_sales_order_header
-- ----------------------------
ALTER TABLE "public"."trans_inventory_subsidiary_sales_order_header" ADD CONSTRAINT "trans_inventory_subsidiary_sales_order_header_pkey" PRIMARY KEY ("id", "id_trans");

-- ----------------------------
-- Primary Key structure for table trans_inventory_subsidiary_sales_order_header_before_perbaikan
-- ----------------------------
ALTER TABLE "public"."trans_inventory_subsidiary_sales_order_header_before_perbaikan" ADD CONSTRAINT "trans_inventory_subsidiary_sales_order_header_copy3_pkey" PRIMARY KEY ("id", "id_trans");

-- ----------------------------
-- Primary Key structure for table trans_inventory_subsidiary_sales_order_header_copy1
-- ----------------------------
ALTER TABLE "public"."trans_inventory_subsidiary_sales_order_header_copy1" ADD CONSTRAINT "trans_inventory_subsidiary_sales_order_header_copy1_pkey" PRIMARY KEY ("id", "id_trans");

-- ----------------------------
-- Primary Key structure for table trans_inventory_subsidiary_sales_order_header_copy2
-- ----------------------------
ALTER TABLE "public"."trans_inventory_subsidiary_sales_order_header_copy2" ADD CONSTRAINT "trans_inventory_subsidiary_sales_order_header_copy2_pkey" PRIMARY KEY ("id", "id_trans");

-- ----------------------------
-- Triggers structure for table trans_sales_order_paid_payment
-- ----------------------------
CREATE TRIGGER "rekon_payment" AFTER INSERT ON "public"."trans_sales_order_paid_payment"
FOR EACH ROW
EXECUTE PROCEDURE "public"."reconsile_inv"();

-- ----------------------------
-- Triggers structure for table trans_sales_order_paid_payment_copy1
-- ----------------------------
CREATE TRIGGER "rekon_payment" AFTER INSERT ON "public"."trans_sales_order_paid_payment_copy1"
FOR EACH ROW
EXECUTE PROCEDURE "public"."reconsile_inv"();

-- ----------------------------
-- Triggers structure for table trans_sales_order_paid_payment_copy2
-- ----------------------------
CREATE TRIGGER "rekon_payment" AFTER INSERT ON "public"."trans_sales_order_paid_payment_copy2"
FOR EACH ROW
EXECUTE PROCEDURE "public"."reconsile_inv"();

-- ----------------------------
-- Triggers structure for table trans_sales_order_paid_payment_copy3
-- ----------------------------
CREATE TRIGGER "rekon_payment" AFTER INSERT ON "public"."trans_sales_order_paid_payment_copy3"
FOR EACH ROW
EXECUTE PROCEDURE "public"."reconsile_inv"();

-- ----------------------------
-- Triggers structure for table trans_sales_order_paid_payment_copy4
-- ----------------------------
CREATE TRIGGER "rekon_payment" AFTER INSERT ON "public"."trans_sales_order_paid_payment_copy4"
FOR EACH ROW
EXECUTE PROCEDURE "public"."reconsile_inv"();

-- ----------------------------
-- Triggers structure for table trans_sales_order_paid_payment_copy5
-- ----------------------------
CREATE TRIGGER "rekon_payment" AFTER INSERT ON "public"."trans_sales_order_paid_payment_copy5"
FOR EACH ROW
EXECUTE PROCEDURE "public"."reconsile_inv"();

-- ----------------------------
-- Triggers structure for table trans_sales_order_paid_payment_copy6
-- ----------------------------
CREATE TRIGGER "rekon_payment" AFTER INSERT ON "public"."trans_sales_order_paid_payment_copy6"
FOR EACH ROW
EXECUTE PROCEDURE "public"."reconsile_inv"();

-- ----------------------------
-- Triggers structure for table trans_sales_order_paid_payment_copy7
-- ----------------------------
CREATE TRIGGER "rekon_payment" AFTER INSERT ON "public"."trans_sales_order_paid_payment_copy7"
FOR EACH ROW
EXECUTE PROCEDURE "public"."reconsile_inv"();

-- ----------------------------
-- Triggers structure for table trans_sales_order_paid_payment_copy8
-- ----------------------------
CREATE TRIGGER "rekon_payment" AFTER INSERT ON "public"."trans_sales_order_paid_payment_copy8"
FOR EACH ROW
EXECUTE PROCEDURE "public"."reconsile_inv"();

-- ----------------------------
-- Triggers structure for table trans_sales_order_paid_payment_copy_backup
-- ----------------------------
CREATE TRIGGER "rekon_payment" AFTER INSERT ON "public"."trans_sales_order_paid_payment_copy_backup"
FOR EACH ROW
EXECUTE PROCEDURE "public"."reconsile_inv"();

-- ----------------------------
-- Primary Key structure for table trans_sales_order_paid_payment_inv
-- ----------------------------
ALTER TABLE "public"."trans_sales_order_paid_payment_inv" ADD CONSTRAINT "trans_sales_order_paid_payment_inv_copy5_pkey" PRIMARY KEY ("id_");

-- ----------------------------
-- Primary Key structure for table trans_sales_order_paid_payment_inv_copy1
-- ----------------------------
ALTER TABLE "public"."trans_sales_order_paid_payment_inv_copy1" ADD CONSTRAINT "trans_sales_order_paid_payment_inv_copy1_pkey" PRIMARY KEY ("id_");

-- ----------------------------
-- Primary Key structure for table trans_sales_order_paid_payment_inv_copy2
-- ----------------------------
ALTER TABLE "public"."trans_sales_order_paid_payment_inv_copy2" ADD CONSTRAINT "trans_sales_order_paid_payment_inv_copy2_pkey" PRIMARY KEY ("id_");

-- ----------------------------
-- Primary Key structure for table trans_sales_order_paid_payment_inv_copy3
-- ----------------------------
ALTER TABLE "public"."trans_sales_order_paid_payment_inv_copy3" ADD CONSTRAINT "trans_sales_order_paid_payment_inv_copy3_pkey" PRIMARY KEY ("id_");

-- ----------------------------
-- Primary Key structure for table trans_sales_order_paid_payment_inv_copy4
-- ----------------------------
ALTER TABLE "public"."trans_sales_order_paid_payment_inv_copy4" ADD CONSTRAINT "trans_sales_order_paid_payment_inv_copy4_pkey" PRIMARY KEY ("id_");

-- ----------------------------
-- Primary Key structure for table trans_sales_order_paid_payment_inv_copy5
-- ----------------------------
ALTER TABLE "public"."trans_sales_order_paid_payment_inv_copy5" ADD CONSTRAINT "trans_sales_order_paid_payment_inv_copy5_pkey1" PRIMARY KEY ("id_");

-- ----------------------------
-- Primary Key structure for table trans_sales_order_paid_payment_inv_copy6
-- ----------------------------
ALTER TABLE "public"."trans_sales_order_paid_payment_inv_copy6" ADD CONSTRAINT "trans_sales_order_paid_payment_inv_copy6_pkey" PRIMARY KEY ("id_");

-- ----------------------------
-- Primary Key structure for table trans_sales_order_paid_payment_inv_copy7
-- ----------------------------
ALTER TABLE "public"."trans_sales_order_paid_payment_inv_copy7" ADD CONSTRAINT "trans_sales_order_paid_payment_inv_copy7_pkey" PRIMARY KEY ("id_");

-- ----------------------------
-- Primary Key structure for table trans_sales_order_paid_payment_inv_copy_backup
-- ----------------------------
ALTER TABLE "public"."trans_sales_order_paid_payment_inv_copy_backup" ADD CONSTRAINT "trans_sales_order_paid_payment_inv_copy8_pkey" PRIMARY KEY ("id_");

-- ----------------------------
-- Primary Key structure for table trans_sales_order_paid_payment_inv_obsolete
-- ----------------------------
ALTER TABLE "public"."trans_sales_order_paid_payment_inv_obsolete" ADD CONSTRAINT "trans_sales_order_paid_payment_inv_pkey" PRIMARY KEY ("id_");

-- ----------------------------
-- Primary Key structure for table va_number_record
-- ----------------------------
ALTER TABLE "public"."va_number_record" ADD CONSTRAINT "va_number_record_pkey" PRIMARY KEY ("va_number");

-- ----------------------------
-- Primary Key structure for table version_apps
-- ----------------------------
ALTER TABLE "public"."version_apps" ADD CONSTRAINT "version_apps_pkey" PRIMARY KEY ("version_id");
