/*
 Navicat Premium Dump SQL

 Source Server         : Localhost
 Source Server Type    : MySQL
 Source Server Version : 80030 (8.0.30)
 Source Host           : localhost:3306
 Source Schema         : data_mart

 Target Server Type    : MySQL
 Target Server Version : 80030 (8.0.30)
 File Encoding         : 65001

 Date: 25/11/2025 09:53:31
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for agg_brand_summary
-- ----------------------------
DROP TABLE IF EXISTS `agg_brand_summary`;
CREATE TABLE `agg_brand_summary`  (
  `brand_sk` int NOT NULL AUTO_INCREMENT,
  `business_key` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `Hãng:` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `product_count` int NULL DEFAULT 0,
  `min_price` bigint NULL DEFAULT NULL,
  `max_price` bigint NULL DEFAULT NULL,
  `avg_price` decimal(18, 2) NULL DEFAULT NULL,
  `last_aggregated_at` datetime NULL DEFAULT NULL,
  `effective_from` datetime NOT NULL,
  `effective_to` datetime NULL DEFAULT NULL,
  `is_current` tinyint(1) NOT NULL DEFAULT 1,
  `inserted_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`brand_sk`) USING BTREE,
  INDEX `idx_brand_bk`(`business_key` ASC) USING BTREE,
  INDEX `idx_brand_current`(`is_current` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 9 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for agg_crawl_daily
-- ----------------------------
DROP TABLE IF EXISTS `agg_crawl_daily`;
CREATE TABLE `agg_crawl_daily`  (
  `crawl_date` date NOT NULL,
  `source_file` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `total_rows` int NULL DEFAULT 0,
  `distinct_products` int NULL DEFAULT 0,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`crawl_date`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for agg_price_history
-- ----------------------------
DROP TABLE IF EXISTS `agg_price_history`;
CREATE TABLE `agg_price_history`  (
  `price_sk` int NOT NULL AUTO_INCREMENT,
  `business_key` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `Tên sản phẩm` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `Giá` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `Source` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `source_file` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `effective_from` datetime NOT NULL,
  `effective_to` datetime NULL DEFAULT NULL,
  `is_current` tinyint(1) NOT NULL DEFAULT 1,
  `inserted_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`price_sk`) USING BTREE,
  INDEX `idx_price_bk`(`business_key` ASC) USING BTREE,
  INDEX `idx_price_current`(`is_current` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 43 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for agg_product_summary
-- ----------------------------
DROP TABLE IF EXISTS `agg_product_summary`;
CREATE TABLE `agg_product_summary`  (
  `product_sk` int NOT NULL AUTO_INCREMENT,
  `business_key` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `Tên sản phẩm` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `Giá` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `Source` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `Hệ điều hành:` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `Chip xử lý (CPU):` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `Tốc độ CPU:` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `Chip đồ họa (GPU):` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `RAM:` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `Dung lượng lưu trữ:` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `Dung lượng còn lại (khả dụng) khoảng:` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `Hãng:` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `created_at` datetime NULL DEFAULT NULL,
  `source_file` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  `effective_from` datetime NOT NULL,
  `effective_to` datetime NULL DEFAULT NULL,
  `is_current` tinyint(1) NOT NULL DEFAULT 1,
  `inserted_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`product_sk`) USING BTREE,
  INDEX `idx_product_bk`(`business_key` ASC) USING BTREE,
  INDEX `idx_product_current`(`is_current` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 43 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Procedure structure for sp_load_agg_brand_summary
-- ----------------------------
DROP PROCEDURE IF EXISTS `sp_load_agg_brand_summary`;
delimiter ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_agg_brand_summary`()
BEGIN
  DECLARE done INT DEFAULT 0;
  DECLARE v_brand TEXT;

  DECLARE cur1 CURSOR FOR
    SELECT DISTINCT `Hãng:` FROM `staging`.`staging.rawtgdd`;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  START TRANSACTION;
  OPEN cur1;

  brand_loop: LOOP
    FETCH cur1 INTO v_brand;
    IF done = 1 THEN LEAVE brand_loop; END IF;

    SET v_brand = COALESCE(v_brand, '');

    UPDATE `data_mart`.`agg_brand_summary`
      SET effective_to = NOW(), is_current = 0
    WHERE business_key = LEFT(v_brand,190) AND is_current = 1;

    INSERT INTO `data_mart`.`agg_brand_summary`
      (business_key, `Hãng:`, product_count, min_price, max_price, avg_price,
       last_aggregated_at, effective_from, is_current)
    SELECT
      LEFT(v_brand,190),
      v_brand,
      COUNT(1),
  MIN(NULLIF(REGEXP_REPLACE(`Giá`, '[^0-9.]', ''), '') + 0),
  MAX(NULLIF(REGEXP_REPLACE(`Giá`, '[^0-9.]', ''), '') + 0),
  AVG(NULLIF(REGEXP_REPLACE(`Giá`, '[^0-9.]', ''), '') + 0),

      NOW(), NOW(), 1
    FROM `staging`.`staging.rawtgdd`
    WHERE `Hãng:` = v_brand;
  END LOOP;

  CLOSE cur1;
  COMMIT;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for sp_load_agg_crawl_daily
-- ----------------------------
DROP PROCEDURE IF EXISTS `sp_load_agg_crawl_daily`;
delimiter ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_agg_crawl_daily`()
BEGIN
  DECLARE v_date DATE;
  DECLARE v_total INT;
  DECLARE v_distinct INT;
  DECLARE v_file TEXT;

  SET v_date = CURDATE();

  SELECT COUNT(1), COUNT(DISTINCT `Source`), GROUP_CONCAT(DISTINCT `source_file` SEPARATOR '; ')
    INTO v_total, v_distinct, v_file
  FROM `staging`.`staging.rawtgdd`;

  INSERT INTO `data_mart`.`agg_crawl_daily`
    (crawl_date, source_file, total_rows, distinct_products, created_at)
  VALUES
    (v_date, v_file, v_total, v_distinct, NOW())
  ON DUPLICATE KEY UPDATE
    source_file = VALUES(source_file),
    total_rows = VALUES(total_rows),
    distinct_products = VALUES(distinct_products),
    created_at = VALUES(created_at);
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for sp_load_agg_price_history
-- ----------------------------
DROP PROCEDURE IF EXISTS `sp_load_agg_price_history`;
delimiter ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_agg_price_history`()
BEGIN
  DECLARE done INT DEFAULT 0;

  DECLARE v_Source TEXT;
  DECLARE v_Gia VARCHAR(100);

  DECLARE cur1 CURSOR FOR
    SELECT `Source`, `Giá` FROM `staging`.`staging.rawtgdd`;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  START TRANSACTION;
  OPEN cur1;

  read_loop: LOOP
    FETCH cur1 INTO v_Source, v_Gia;
    IF done = 1 THEN LEAVE read_loop; END IF;

    SET v_Source = LEFT(COALESCE(v_Source,''),190);

    IF EXISTS(SELECT 1 FROM `data_mart`.`agg_price_history`
              WHERE business_key = v_Source AND is_current = 1) THEN

      IF NOT EXISTS(
        SELECT 1 FROM `data_mart`.`agg_price_history`
        WHERE business_key = v_Source AND is_current = 1
          AND (`Giá` <=> v_Gia)
      ) THEN

        UPDATE `data_mart`.`agg_price_history`
          SET effective_to = NOW(), is_current = 0
        WHERE business_key = v_Source AND is_current = 1;

        INSERT INTO `data_mart`.`agg_price_history`
        (business_key, `Tên sản phẩm`, `Giá`, `Source`, `source_file`, effective_from, is_current)
        SELECT LEFT(`Source`,190), `Tên sản phẩm`, `Giá`, `Source`, `source_file`, NOW(), 1
        FROM `staging`.`staging.rawtgdd`
        WHERE `Source` = v_Source LIMIT 1;
      END IF;

    ELSE
      INSERT INTO `data_mart`.`agg_price_history`
      (business_key, `Tên sản phẩm`, `Giá`, `Source`, `source_file`, effective_from, is_current)
      SELECT LEFT(`Source`,190), `Tên sản phẩm`, `Giá`, `Source`, `source_file`, NOW(), 1
      FROM `staging`.`staging.rawtgdd`
      WHERE `Source` = v_Source LIMIT 1;
    END IF;

  END LOOP;

  CLOSE cur1;
  COMMIT;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for sp_load_agg_product_summary
-- ----------------------------
DROP PROCEDURE IF EXISTS `sp_load_agg_product_summary`;
delimiter ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_agg_product_summary`()
BEGIN
  DECLARE done INT DEFAULT 0;

  DECLARE v_Source TEXT;
  DECLARE v_Ten TEXT;
  DECLARE v_Gia VARCHAR(100);
  DECLARE v_Hang TEXT;
  DECLARE v_chip TEXT;
  DECLARE v_ram TEXT;
  DECLARE v_storage TEXT;
  DECLARE v_source_file TEXT;

  DECLARE cur1 CURSOR FOR
    SELECT `Source`, `Tên sản phẩm`, `Giá`, `Hãng:`, `Chip xử lý (CPU):`,
           `RAM:`, `Dung lượng lưu trữ:`, `source_file`
    FROM `staging`.`staging.rawtgdd`;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  START TRANSACTION;
  OPEN cur1;

  read_loop: LOOP
    FETCH cur1 INTO v_Source, v_Ten, v_Gia, v_Hang, v_chip, v_ram, v_storage, v_source_file;
    IF done = 1 THEN LEAVE read_loop; END IF;

    SET v_Source = LEFT(COALESCE(v_Source,''),190);

    IF EXISTS (SELECT 1 FROM `data_mart`.`agg_product_summary`
               WHERE business_key = v_Source AND is_current = 1) THEN

      IF NOT EXISTS (
        SELECT 1 FROM `data_mart`.`agg_product_summary`
        WHERE business_key = v_Source AND is_current = 1
          AND (`Tên sản phẩm` <=> v_Ten)
          AND (`Giá` <=> v_Gia)
          AND (`Hãng:` <=> v_Hang)
          AND (`Chip xử lý (CPU):` <=> v_chip)
          AND (`RAM:` <=> v_ram)
          AND (`Dung lượng lưu trữ:` <=> v_storage)
      ) THEN

        UPDATE `data_mart`.`agg_product_summary`
        SET effective_to = NOW(), is_current = 0
        WHERE business_key = v_Source AND is_current = 1;

        INSERT INTO `data_mart`.`agg_product_summary`
        (business_key, `Tên sản phẩm`, `Giá`, `Source`, `Hệ điều hành:`,
         `Chip xử lý (CPU):`, `Tốc độ CPU:`, `Chip đồ họa (GPU):`,
         `RAM:`, `Dung lượng lưu trữ:`, `Dung lượng còn lại (khả dụng) khoảng:`,
         `Hãng:`, created_at, source_file, effective_from, is_current)
        SELECT LEFT(`Source`,190), `Tên sản phẩm`, `Giá`, `Source`, `Hệ điều hành:`,
               `Chip xử lý (CPU):`, `Tốc độ CPU:`, `Chip đồ họa (GPU):`,
               `RAM:`, `Dung lượng lưu trữ:`, `Dung lượng còn lại (khả dụng) khoảng:`,
               `Hãng:`, NULL, `source_file`, NOW(), 1
        FROM `staging`.`staging.rawtgdd`
        WHERE `Source` = v_Source LIMIT 1;
      END IF;

    ELSE
      INSERT INTO `data_mart`.`agg_product_summary`
      (business_key, `Tên sản phẩm`, `Giá`, `Source`, `Hệ điều hành:`,
       `Chip xử lý (CPU):`, `Tốc độ CPU:`, `Chip đồ họa (GPU):`,
       `RAM:`, `Dung lượng lưu trữ:`, `Dung lượng còn lại (khả dụng) khoảng:`,
       `Hãng:`, created_at, source_file, effective_from, is_current)
      SELECT LEFT(`Source`,190), `Tên sản phẩm`, `Giá`, `Source`, `Hệ điều hành:`,
             `Chip xử lý (CPU):`, `Tốc độ CPU:`, `Chip đồ họa (GPU):`,
             `RAM:`, `Dung lượng lưu trữ:`, `Dung lượng còn lại (khả dụng) khoảng:`,
             `Hãng:`, NULL, `source_file`, NOW(), 1
      FROM `staging`.`staging.rawtgdd`
      WHERE `Source` = v_Source LIMIT 1;
    END IF;

  END LOOP;

  CLOSE cur1;
  COMMIT;
END
;;
delimiter ;

SET FOREIGN_KEY_CHECKS = 1;
