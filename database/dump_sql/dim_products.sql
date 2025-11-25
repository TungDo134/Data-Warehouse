/*
 Navicat Premium Dump SQL

 Source Server         : Localhost
 Source Server Type    : MySQL
 Source Server Version : 80030 (8.0.30)
 Source Host           : localhost:3306
 Source Schema         : data_storage

 Target Server Type    : MySQL
 Target Server Version : 80030 (8.0.30)
 File Encoding         : 65001

 Date: 24/11/2025 23:29:45
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for dim_products
-- ----------------------------
DROP TABLE IF EXISTS `dim_products`;
CREATE TABLE `dim_products`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `product_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `product_name` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `product_price` decimal(18, 2) NULL DEFAULT NULL,
  `operating_system` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `cpu_chip` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `cpu_speed` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `gpu_chip` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `ram` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `storage_capacity` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `available_storage` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `contacts` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `rear_camera_resolution` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `rear_camera_video` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `rear_camera_flash` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `rear_camera_features` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `front_camera_resolution` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `front_camera_features` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `display_technology` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `display_resolution` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `screen_size` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `max_brightness` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `touch_glass` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `battery_capacity` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `battery_type` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `max_charging_support` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `battery_technology` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `security_features` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `special_features` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `water_dust_resistance` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `voice_recorder` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `video_playback` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `music_playback` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `mobile_network` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `sim_type` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `wifi_support` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `gps_support` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `bluetooth_version` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `cong_ket_noi/sac` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `headphone_jack` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `other_connections` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `design_style` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `material` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `dimensions_weight` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `release_date` date NULL DEFAULT NULL,
  `brand` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `dt_expired` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `product_id`(`product_id` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 30 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
