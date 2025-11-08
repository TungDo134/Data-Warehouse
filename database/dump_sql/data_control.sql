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

 Date: 08/11/2025 18:23:40
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
INSERT INTO `data_control.config` VALUES (1, 'TGDD', 'https://www.thegioididong.com/', 'Web', 'staging.rawtgdd', NULL, 1, NULL, NULL, '2025-10-26 10:11:07', '2025-11-04 21:49:53', NULL, NULL, NULL, 1, 6, 'D:\\Workspace-Python\\Data-Warehouse\\Crawl Data');

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
  `process_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `source_system` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `target_table` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `start_time` timestamp NULL DEFAULT NULL,
  `end_time` timestamp NULL DEFAULT NULL,
  `duration_seconds` int NULL DEFAULT NULL,
  `status` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `rows_extracted` int NULL DEFAULT NULL,
  `rows_transformed` int NULL DEFAULT NULL,
  `rows_loaded` int NULL DEFAULT NULL,
  `error_message` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `log_level` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `created_by` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  PRIMARY KEY (`log_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of data_control.logs
-- ----------------------------

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
