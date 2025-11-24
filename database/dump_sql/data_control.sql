/*
 Navicat Premium Dump SQL

 Source Server         : Localhost
 Source Server Type    : MySQL
 Source Server Version : 80030 (8.0.30)
 Source Host           : localhost:3306
 Source Schema         : data_control

 Target Server Type    : MySQL
 Target Server Version : 80030 (8.0.30)
 File Encoding         : 65001

 Date: 24/11/2025 21:44:03
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for data_control.config
-- ----------------------------
DROP TABLE IF EXISTS `data_control.config`;
CREATE TABLE `data_control.config`  (
  `config_id` int NOT NULL AUTO_INCREMENT,
  `config_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `source_url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `source_type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `target_table` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `schedule` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `is_active` tinyint(1) NULL DEFAULT 1,
  `last_run_time` datetime NULL DEFAULT NULL,
  `next_run_time` datetime NULL DEFAULT NULL,
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `extra_params` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `created_by` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `max_clicks` int NULL DEFAULT NULL,
  `record_limit` int NULL DEFAULT NULL,
  `src_folder` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  PRIMARY KEY (`config_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of data_control.config
-- ----------------------------
INSERT INTO `data_control.config` VALUES (1, 'TGDD', 'https://www.thegioididong.com/', 'Web', 'staging.rawtgdd', NULL, 1, NULL, NULL, '2025-10-26 10:11:07', '2025-11-18 09:33:43', NULL, NULL, NULL, 1, 6, 'D:\\Workspace-Python\\Data-Warehouse\\Crawl Data');

-- ----------------------------
-- Table structure for data_control.field_mapping
-- ----------------------------
DROP TABLE IF EXISTS `data_control.field_mapping`;
CREATE TABLE `data_control.field_mapping`  (
  `mapping_id` int NOT NULL AUTO_INCREMENT,
  `source_system` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `source_field` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `target_field` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `data_type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `transformation_rule` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `is_active` tinyint(1) NULL DEFAULT 1,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`mapping_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of data_control.field_mapping
-- ----------------------------

-- ----------------------------
-- Table structure for data_control.logs
-- ----------------------------
DROP TABLE IF EXISTS `data_control.logs`;
CREATE TABLE `data_control.logs`  (
  `log_id` int NOT NULL AUTO_INCREMENT,
  `process_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Tên process: EXTRACT, TRANSFORM, LOAD...',
  `source_system` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Nguồn dữ liệu: TGDĐ, FPT Shop...',
  `target_table` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Bảng đích: STAGING, ODS, DW...',
  `status` enum('SUCCESS','FAILED','RUNNING') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Trạng thái',
  `message` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL COMMENT 'Thông điệp bổ sung',
  `rows_affected` int NULL DEFAULT NULL COMMENT 'Số dòng được xử lý',
  `log_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian log',
  PRIMARY KEY (`log_id`) USING BTREE,
  INDEX `idx_process`(`process_name` ASC) USING BTREE,
  INDEX `idx_status`(`status` ASC) USING BTREE,
  INDEX `idx_log_time`(`log_time` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 70 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = 'Bảng log tổng quan cho ETL process' ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Records of data_control.logs
-- ----------------------------
INSERT INTO `data_control.logs` VALUES (1, 'EXTRACT', 'TGDĐ', 'STAGING', 'SUCCESS', 'Đã crawl dữ liệu từ TGDĐ thành công', 18, '2025-11-23 23:13:36');
INSERT INTO `data_control.logs` VALUES (2, 'load_staging', 'TGDĐ', 'staging_product', 'SUCCESS', '', 18, '2025-11-23 23:13:36');
INSERT INTO `data_control.logs` VALUES (3, 'LOAD_STAGING', 'TGDĐ', 'STAGING', 'SUCCESS', 'Đã load vào Staging thành công', 18, '2025-11-23 23:13:36');
INSERT INTO `data_control.logs` VALUES (4, 'TRANSFORM', 'TGDĐ', 'ODS', 'SUCCESS', 'Đã clean và transform. Loại bỏ 0 sản phẩm', 18, '2025-11-23 23:13:36');
INSERT INTO `data_control.logs` VALUES (5, 'load_ods', 'TGDĐ', 'ods_product', 'SUCCESS', '', 18, '2025-11-23 23:13:36');
INSERT INTO `data_control.logs` VALUES (6, 'LOAD_ODS', 'TGDĐ', 'ODS_PRODUCT', 'SUCCESS', 'Đã load vào ODS thành công', 18, '2025-11-23 23:13:36');
INSERT INTO `data_control.logs` VALUES (7, 'load_dim_product', 'TGDĐ', 'dim_product', 'SUCCESS', '', 18, '2025-11-23 23:13:36');
INSERT INTO `data_control.logs` VALUES (8, 'LOAD_DIM', 'TGDĐ', 'DIM_PRODUCT', 'SUCCESS', 'Đã load vào DIM_PRODUCT thành công', 18, '2025-11-23 23:13:36');
INSERT INTO `data_control.logs` VALUES (9, 'load_fact_price', 'TGDĐ', 'fact_price', 'SUCCESS', '', 18, '2025-11-23 23:13:36');
INSERT INTO `data_control.logs` VALUES (10, 'LOAD_FACT', 'TGDĐ', 'FACT_PRICE', 'SUCCESS', 'Đã load vào FACT_PRICE thành công', 18, '2025-11-23 23:13:36');
INSERT INTO `data_control.logs` VALUES (11, 'PIPELINE_COMPLETE', 'TGDĐ', 'DW', 'SUCCESS', 'ETL Pipeline hoàn thành thành công', 18, '2025-11-23 23:13:36');
INSERT INTO `data_control.logs` VALUES (12, 'EXTRACT', 'TGDĐ', 'STAGING', 'SUCCESS', 'Đã crawl dữ liệu từ TGDĐ thành công', 18, '2025-11-23 23:22:27');
INSERT INTO `data_control.logs` VALUES (13, 'load_staging', 'TGDĐ', 'staging_product', 'SUCCESS', '', 18, '2025-11-23 23:22:27');
INSERT INTO `data_control.logs` VALUES (14, 'LOAD_STAGING', 'TGDĐ', 'STAGING', 'SUCCESS', 'Đã load vào Staging thành công', 18, '2025-11-23 23:22:27');
INSERT INTO `data_control.logs` VALUES (15, 'TRANSFORM', 'TGDĐ', 'ODS', 'SUCCESS', 'Đã clean và transform. Loại bỏ 0 sản phẩm', 18, '2025-11-23 23:22:27');
INSERT INTO `data_control.logs` VALUES (16, 'load_ods', 'TGDĐ', 'ods_product', 'FAILED', '(mysql.connector.errors.ProgrammingError) 1049 (42000): Unknown database \'ods\'\n(Background on this error at: https://sqlalche.me/e/20/f405)', NULL, '2025-11-23 23:22:27');
INSERT INTO `data_control.logs` VALUES (17, 'PIPELINE_FAILED', 'TGDĐ', 'DW', 'FAILED', '(mysql.connector.errors.ProgrammingError) 1049 (42000): Unknown database \'ods\'\n(Background on this error at: https://sqlalche.me/e/20/f405)', NULL, '2025-11-23 23:22:27');
INSERT INTO `data_control.logs` VALUES (18, 'EXTRACT', 'TGDĐ', 'STAGING', 'SUCCESS', 'Đã crawl dữ liệu từ TGDĐ thành công', 18, '2025-11-23 23:23:48');
INSERT INTO `data_control.logs` VALUES (19, 'load_staging', 'TGDĐ', 'staging_product', 'SUCCESS', '', 18, '2025-11-23 23:23:48');
INSERT INTO `data_control.logs` VALUES (20, 'LOAD_STAGING', 'TGDĐ', 'STAGING', 'SUCCESS', 'Đã load vào Staging thành công', 18, '2025-11-23 23:23:48');
INSERT INTO `data_control.logs` VALUES (21, 'TRANSFORM', 'TGDĐ', 'ODS', 'SUCCESS', 'Đã clean và transform. Loại bỏ 0 sản phẩm', 18, '2025-11-23 23:23:48');
INSERT INTO `data_control.logs` VALUES (22, 'load_ods', 'TGDĐ', 'ods_product', 'SUCCESS', '', 18, '2025-11-23 23:23:48');
INSERT INTO `data_control.logs` VALUES (23, 'LOAD_ODS', 'TGDĐ', 'ODS_PRODUCT', 'SUCCESS', 'Đã load vào ODS thành công', 18, '2025-11-23 23:23:48');
INSERT INTO `data_control.logs` VALUES (24, 'load_dim_product', 'TGDĐ', 'dim_product', 'FAILED', '(mysql.connector.errors.ProgrammingError) 1049 (42000): Unknown database \'dw\'\n(Background on this error at: https://sqlalche.me/e/20/f405)', NULL, '2025-11-23 23:23:48');
INSERT INTO `data_control.logs` VALUES (25, 'PIPELINE_FAILED', 'TGDĐ', 'DW', 'FAILED', '(mysql.connector.errors.ProgrammingError) 1049 (42000): Unknown database \'dw\'\n(Background on this error at: https://sqlalche.me/e/20/f405)', NULL, '2025-11-23 23:23:48');
INSERT INTO `data_control.logs` VALUES (26, 'EXTRACT', 'TGDĐ', 'STAGING', 'SUCCESS', 'Đã crawl dữ liệu từ TGDĐ thành công', 18, '2025-11-23 23:25:11');
INSERT INTO `data_control.logs` VALUES (27, 'load_staging', 'TGDĐ', 'staging_product', 'SUCCESS', '', 18, '2025-11-23 23:25:12');
INSERT INTO `data_control.logs` VALUES (28, 'LOAD_STAGING', 'TGDĐ', 'STAGING', 'SUCCESS', 'Đã load vào Staging thành công', 18, '2025-11-23 23:25:12');
INSERT INTO `data_control.logs` VALUES (29, 'TRANSFORM', 'TGDĐ', 'ODS', 'SUCCESS', 'Đã clean và transform. Loại bỏ 0 sản phẩm', 18, '2025-11-23 23:25:12');
INSERT INTO `data_control.logs` VALUES (30, 'load_ods', 'TGDĐ', 'ods_product', 'SUCCESS', '', 18, '2025-11-23 23:25:12');
INSERT INTO `data_control.logs` VALUES (31, 'LOAD_ODS', 'TGDĐ', 'ODS_PRODUCT', 'SUCCESS', 'Đã load vào ODS thành công', 18, '2025-11-23 23:25:12');
INSERT INTO `data_control.logs` VALUES (32, 'load_dim_product', 'TGDĐ', 'dim_product', 'SUCCESS', '', 18, '2025-11-23 23:25:12');
INSERT INTO `data_control.logs` VALUES (33, 'LOAD_DIM', 'TGDĐ', 'DIM_PRODUCT', 'SUCCESS', 'Đã load vào DIM_PRODUCT thành công', 18, '2025-11-23 23:25:12');
INSERT INTO `data_control.logs` VALUES (34, 'load_fact_price', 'TGDĐ', 'fact_price', 'SUCCESS', '', 18, '2025-11-23 23:25:12');
INSERT INTO `data_control.logs` VALUES (35, 'LOAD_FACT', 'TGDĐ', 'FACT_PRICE', 'SUCCESS', 'Đã load vào FACT_PRICE thành công', 18, '2025-11-23 23:25:12');
INSERT INTO `data_control.logs` VALUES (36, 'PIPELINE_COMPLETE', 'TGDĐ', 'DW', 'SUCCESS', 'ETL Pipeline hoàn thành thành công', 18, '2025-11-23 23:25:12');
INSERT INTO `data_control.logs` VALUES (37, 'EXTRACT', 'TGDĐ', 'STAGING', 'SUCCESS', 'Đã crawl dữ liệu từ TGDĐ thành công', 18, '2025-11-24 15:04:23');
INSERT INTO `data_control.logs` VALUES (38, 'load_staging', 'TGDĐ', 'staging_product', 'SUCCESS', '', 18, '2025-11-24 15:04:23');
INSERT INTO `data_control.logs` VALUES (39, 'LOAD_STAGING', 'TGDĐ', 'STAGING', 'SUCCESS', 'Đã load vào Staging thành công', 18, '2025-11-24 15:04:23');
INSERT INTO `data_control.logs` VALUES (40, 'TRANSFORM', 'TGDĐ', 'ODS', 'SUCCESS', 'Đã clean và transform. Loại bỏ 0 sản phẩm', 18, '2025-11-24 15:04:23');
INSERT INTO `data_control.logs` VALUES (41, 'load_ods', 'TGDĐ', 'ods_product', 'SUCCESS', '', 18, '2025-11-24 15:04:23');
INSERT INTO `data_control.logs` VALUES (42, 'LOAD_ODS', 'TGDĐ', 'ODS_PRODUCT', 'SUCCESS', 'Đã load vào ODS thành công', 18, '2025-11-24 15:04:23');
INSERT INTO `data_control.logs` VALUES (43, 'load_dim_product', 'TGDĐ', 'dim_product', 'SUCCESS', '', 18, '2025-11-24 15:04:23');
INSERT INTO `data_control.logs` VALUES (44, 'LOAD_DIM', 'TGDĐ', 'DIM_PRODUCT', 'SUCCESS', 'Đã load vào DIM_PRODUCT thành công', 18, '2025-11-24 15:04:23');
INSERT INTO `data_control.logs` VALUES (45, 'load_fact_price', 'TGDĐ', 'fact_price', 'SUCCESS', '', 18, '2025-11-24 15:04:23');
INSERT INTO `data_control.logs` VALUES (46, 'LOAD_FACT', 'TGDĐ', 'FACT_PRICE', 'SUCCESS', 'Đã load vào FACT_PRICE thành công', 18, '2025-11-24 15:04:23');
INSERT INTO `data_control.logs` VALUES (47, 'PIPELINE_COMPLETE', 'TGDĐ', 'DW', 'SUCCESS', 'ETL Pipeline hoàn thành thành công', 18, '2025-11-24 15:04:23');
INSERT INTO `data_control.logs` VALUES (48, 'EXTRACT', 'TGDĐ', 'STAGING', 'SUCCESS', 'Đã crawl dữ liệu từ TGDĐ thành công', 18, '2025-11-24 21:12:06');
INSERT INTO `data_control.logs` VALUES (49, 'load_staging', 'TGDĐ', 'staging_product', 'SUCCESS', '', 18, '2025-11-24 21:12:06');
INSERT INTO `data_control.logs` VALUES (50, 'LOAD_STAGING', 'TGDĐ', 'STAGING', 'SUCCESS', 'Đã load vào Staging thành công', 18, '2025-11-24 21:12:06');
INSERT INTO `data_control.logs` VALUES (51, 'TRANSFORM', 'TGDĐ', 'ODS', 'SUCCESS', 'Đã clean và transform. Loại bỏ 0 sản phẩm', 18, '2025-11-24 21:12:06');
INSERT INTO `data_control.logs` VALUES (52, 'load_ods', 'TGDĐ', 'ods_product', 'SUCCESS', '', 18, '2025-11-24 21:12:06');
INSERT INTO `data_control.logs` VALUES (53, 'LOAD_ODS', 'TGDĐ', 'ODS_PRODUCT', 'SUCCESS', 'Đã load vào ODS thành công', 18, '2025-11-24 21:12:06');
INSERT INTO `data_control.logs` VALUES (54, 'load_dim_product', 'TGDĐ', 'dim_product', 'SUCCESS', '', 18, '2025-11-24 21:12:06');
INSERT INTO `data_control.logs` VALUES (55, 'LOAD_DIM', 'TGDĐ', 'DIM_PRODUCT', 'SUCCESS', 'Đã load vào DIM_PRODUCT thành công', 18, '2025-11-24 21:12:06');
INSERT INTO `data_control.logs` VALUES (56, 'load_fact_price', 'TGDĐ', 'fact_price', 'SUCCESS', '', 18, '2025-11-24 21:12:06');
INSERT INTO `data_control.logs` VALUES (57, 'LOAD_FACT', 'TGDĐ', 'FACT_PRICE', 'SUCCESS', 'Đã load vào FACT_PRICE thành công', 18, '2025-11-24 21:12:06');
INSERT INTO `data_control.logs` VALUES (58, 'PIPELINE_COMPLETE', 'TGDĐ', 'DW', 'SUCCESS', 'ETL Pipeline hoàn thành thành công', 18, '2025-11-24 21:12:06');
INSERT INTO `data_control.logs` VALUES (59, 'EXTRACT', 'TGDĐ', 'STAGING', 'SUCCESS', 'Đã crawl dữ liệu từ TGDĐ thành công', 18, '2025-11-24 21:20:10');
INSERT INTO `data_control.logs` VALUES (60, 'load_staging', 'TGDĐ', 'staging_product', 'SUCCESS', '', 18, '2025-11-24 21:20:10');
INSERT INTO `data_control.logs` VALUES (61, 'LOAD_STAGING', 'TGDĐ', 'STAGING', 'SUCCESS', 'Đã load vào Staging thành công', 18, '2025-11-24 21:20:10');
INSERT INTO `data_control.logs` VALUES (62, 'TRANSFORM', 'TGDĐ', 'ODS', 'SUCCESS', 'Đã clean và transform. Loại bỏ 0 sản phẩm', 18, '2025-11-24 21:20:10');
INSERT INTO `data_control.logs` VALUES (63, 'load_ods', 'TGDĐ', 'ods_product', 'SUCCESS', '', 18, '2025-11-24 21:20:10');
INSERT INTO `data_control.logs` VALUES (64, 'LOAD_ODS', 'TGDĐ', 'ODS_PRODUCT', 'SUCCESS', 'Đã load vào ODS thành công', 18, '2025-11-24 21:20:10');
INSERT INTO `data_control.logs` VALUES (65, 'load_dim_product', 'TGDĐ', 'dim_product', 'SUCCESS', '', 18, '2025-11-24 21:20:10');
INSERT INTO `data_control.logs` VALUES (66, 'LOAD_DIM', 'TGDĐ', 'DIM_PRODUCT', 'SUCCESS', 'Đã load vào DIM_PRODUCT thành công', 18, '2025-11-24 21:20:10');
INSERT INTO `data_control.logs` VALUES (67, 'load_fact_price', 'TGDĐ', 'fact_price', 'SUCCESS', '', 18, '2025-11-24 21:20:10');
INSERT INTO `data_control.logs` VALUES (68, 'LOAD_FACT', 'TGDĐ', 'FACT_PRICE', 'SUCCESS', 'Đã load vào FACT_PRICE thành công', 18, '2025-11-24 21:20:10');
INSERT INTO `data_control.logs` VALUES (69, 'PIPELINE_COMPLETE', 'TGDĐ', 'DW', 'SUCCESS', 'ETL Pipeline hoàn thành thành công', 18, '2025-11-24 21:20:10');

-- ----------------------------
-- Table structure for data_control.metadata
-- ----------------------------
DROP TABLE IF EXISTS `data_control.metadata`;
CREATE TABLE `data_control.metadata`  (
  `metadata_id` char(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT (uuid()),
  `table_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `source_system` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `source_type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `last_updated` timestamp NULL DEFAULT NULL,
  `record_count` int NULL DEFAULT 0,
  `field_count` int NULL DEFAULT 0,
  `mapping_id` int NULL DEFAULT NULL,
  `config_id` int NULL DEFAULT NULL,
  `last_log_id` int NULL DEFAULT NULL,
  `is_active` tinyint(1) NULL DEFAULT 1,
  `owner` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`metadata_id`) USING BTREE,
  INDEX `fk_metadata_mapping`(`mapping_id` ASC) USING BTREE,
  INDEX `fk_metadata_config`(`config_id` ASC) USING BTREE,
  CONSTRAINT `fk_metadata_config` FOREIGN KEY (`config_id`) REFERENCES `data_control.config` (`config_id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_metadata_mapping` FOREIGN KEY (`mapping_id`) REFERENCES `data_control.field_mapping` (`mapping_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of data_control.metadata
-- ----------------------------

SET FOREIGN_KEY_CHECKS = 1;
