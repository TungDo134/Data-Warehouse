
--  PROJECT: DATA MART FOR PRODUCT ANALYTICS
--  LAYER: AGGREGATE LAYER (DIMENSIONAL MODELING + SCD2)
--  PURPOSE:
--      - Lưu lịch sử thay đổi cho sản phẩm, giá, hãng
--      - Tạo bảng tổng hợp theo ngày crawl
--      - ETL bằng Stored Procedure, xử lý SCD2 FULL




-- 1) TẠO DATABASE (nếu chưa tồn tại)


CREATE DATABASE IF NOT EXISTS `data_mart`
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;

USE `data_mart`;



-- 2) BẢNG AGG_PRODUCT_SUMMARY
--      - Lưu lịch sử FULL thông tin sản phẩm theo SCD2
--      - business_key = URL rút gọn


CREATE TABLE IF NOT EXISTS `agg_product_summary` (
  product_sk INT NOT NULL AUTO_INCREMENT,              -- Surrogate Key
  business_key VARCHAR(500) NOT NULL,                  -- Source (URL) <= 190 ký tự, làm khóa nghiệp vụ

  -- Các thuộc tính sản phẩm
  `Tên sản phẩm` TEXT,
  `Giá` VARCHAR(100),
  `Source` TEXT,
  `Hệ điều hành:` TEXT,
  `Chip xử lý (CPU):` TEXT,
  `Tốc độ CPU:` TEXT,
  `Chip đồ họa (GPU):` TEXT,
  `RAM:` TEXT,
  `Dung lượng lưu trữ:` TEXT,
  `Dung lượng còn lại (khả dụng) khoảng:` TEXT,
  `Hãng:` TEXT,

  created_at DATETIME NULL,                            -- Ngày tạo record tại nguồn crawl
  source_file TEXT,                                    -- File nguồn

  -- Trường SCD2
  effective_from DATETIME NOT NULL,                    -- Ngày version này bắt đầu có hiệu lực
  effective_to DATETIME DEFAULT NULL,                  -- Ngày version kết thúc
  is_current TINYINT(1) NOT NULL DEFAULT 1,            -- 1 = hiện tại, 0 = lịch sử

  inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,     -- Audit
  updated_at TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (product_sk),
  INDEX idx_product_bk (business_key),                 -- Tối ưu truy vấn theo business key
  INDEX idx_product_current (is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;




-- 3) BẢNG AGG_PRICE_HISTORY
--      - Lưu lịch sử thay đổi giá theo SCD2


CREATE TABLE IF NOT EXISTS `agg_price_history` (
  price_sk INT NOT NULL AUTO_INCREMENT,
  business_key VARCHAR(200) NOT NULL,                  -- URL rút gọn
  `Tên sản phẩm` TEXT,
  `Giá` VARCHAR(100),
  `Source` TEXT,
  source_file TEXT,

  -- SCD2
  effective_from DATETIME NOT NULL,
  effective_to DATETIME DEFAULT NULL,
  is_current TINYINT(1) NOT NULL DEFAULT 1,

  inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (price_sk),
  INDEX idx_price_bk (business_key),
  INDEX idx_price_current (is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;




-- 4) BẢNG AGG_BRAND_SUMMARY
--      - Thống kê số lượng sản phẩm, min/max price, avg price theo hãng
--      - Dữ liệu snapshot theo SCD2


CREATE TABLE IF NOT EXISTS `agg_brand_summary` (
  brand_sk INT NOT NULL AUTO_INCREMENT,
  business_key VARCHAR(200) NOT NULL,       -- Tên hãng rút gọn
  `Hãng:` TEXT,
  product_count INT DEFAULT 0,              -- Tổng số sản phẩm
  min_price BIGINT DEFAULT NULL,
  max_price BIGINT DEFAULT NULL,
  avg_price DECIMAL(18,2) DEFAULT NULL,
  last_aggregated_at DATETIME DEFAULT NULL, -- Thời điểm tính toán snapshot

  -- SCD2
  effective_from DATETIME NOT NULL,
  effective_to DATETIME DEFAULT NULL,
  is_current TINYINT(1) NOT NULL DEFAULT 1,

  inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (brand_sk),
  INDEX idx_brand_bk (business_key),
  INDEX idx_brand_current (is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;




-- 5) BẢNG AGG_CRAWL_DAILY
--      - Thống kê theo ngày crawl
--      - Không SCD2, chỉ snapshot daily


CREATE TABLE IF NOT EXISTS `agg_crawl_daily` (
  crawl_date DATE NOT NULL,                 -- Ngày crawl
  source_file TEXT,                         -- File chứa dữ liệu crawl của ngày đó
  total_rows INT DEFAULT 0,                 -- Tổng số dòng crawl
  distinct_products INT DEFAULT 0,          -- Số sản phẩm duy nhất
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (crawl_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;




-- 6) STORED PROCEDURE: LOAD agg_product_summary (SCD2 FULL)
--      - So sánh toàn bộ các field quan trọng
--      - Nếu khác -> đóng version cũ + tạo version mới


DELIMITER $$

CREATE PROCEDURE `sp_load_agg_product_summary`()
BEGIN
  DECLARE done INT DEFAULT 0;

  -- Các biến tạm từ cursor
  DECLARE v_Source TEXT;
  DECLARE v_Ten TEXT;
  DECLARE v_Gia VARCHAR(100);
  DECLARE v_Hang TEXT;
  DECLARE v_chip TEXT;
  DECLARE v_ram TEXT;
  DECLARE v_storage TEXT;
  DECLARE v_source_file TEXT;

  -- Cursor đọc staging
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

    -- Chuẩn hóa business key
    SET v_Source = LEFT(COALESCE(v_Source,''),190);

    -- Nếu đã có bản hiện hành
    IF EXISTS (SELECT 1 FROM `data_mart`.`agg_product_summary`
               WHERE business_key = v_Source AND is_current = 1) THEN

      -- Kiểm tra dữ liệu có thay đổi không
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

        -- Đóng bản cũ
        UPDATE `data_mart`.`agg_product_summary`
        SET effective_to = NOW(), is_current = 0
        WHERE business_key = v_Source AND is_current = 1;

        -- Tạo bản mới (version mới)
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
      -- Chưa từng có record → Insert mới (version đầu tiên)
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
END$$

DELIMITER ;




-- 7) STORED PROCEDURE: LOAD PRICE HISTORY (SCD2 CHO GIÁ)


DELIMITER $$

CREATE PROCEDURE `sp_load_agg_price_history`()
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

    -- Nếu đã có bản giá hiện tại
    IF EXISTS(SELECT 1 FROM `data_mart`.`agg_price_history`
              WHERE business_key = v_Source AND is_current = 1) THEN

      -- Kiểm tra thay đổi giá
      IF NOT EXISTS(
        SELECT 1 FROM `data_mart`.`agg_price_history`
        WHERE business_key = v_Source AND is_current = 1
          AND (`Giá` <=> v_Gia)
      ) THEN

        -- Đóng bản giá cũ
        UPDATE `data_mart`.`agg_price_history`
          SET effective_to = NOW(), is_current = 0
        WHERE business_key = v_Source AND is_current = 1;

        -- Tạo bản giá mới
        INSERT INTO `data_mart`.`agg_price_history`
        (business_key, `Tên sản phẩm`, `Giá`, `Source`, `source_file`, effective_from, is_current)
        SELECT LEFT(`Source`,190), `Tên sản phẩm`, `Giá`, `Source`, `source_file`, NOW(), 1
        FROM `staging`.`staging.rawtgdd`
        WHERE `Source` = v_Source LIMIT 1;
      END IF;

    ELSE
      -- Giá mới hoàn toàn
      INSERT INTO `data_mart`.`agg_price_history`
      (business_key, `Tên sản phẩm`, `Giá`, `Source`, `source_file`, effective_from, is_current)
      SELECT LEFT(`Source`,190), `Tên sản phẩm`, `Giá`, `Source`, `source_file`, NOW(), 1
      FROM `staging`.`staging.rawtgdd`
      WHERE `Source` = v_Source LIMIT 1;
    END IF;

  END LOOP;

  CLOSE cur1;
  COMMIT;
END$$

DELIMITER ;




-- 8) PROCEDURE LOAD BRAND SUMMARY (SCD2 SNAPSHOT)


DELIMITER $$

CREATE PROCEDURE `sp_load_agg_brand_summary`()
BEGIN
  DECLARE done INT DEFAULT 0;
  DECLARE v_brand TEXT;

  -- Cursor đọc distinct brand
  DECLARE cur1 CURSOR FOR
    SELECT DISTINCT `Hãng:` FROM `staging`.`staging.rawtgdd`;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  START TRANSACTION;
  OPEN cur1;

  brand_loop: LOOP
    FETCH cur1 INTO v_brand;
    IF done = 1 THEN LEAVE brand_loop; END IF;

    SET v_brand = COALESCE(v_brand, '');

    -- Đóng snapshot cũ
    UPDATE `data_mart`.`agg_brand_summary`
      SET effective_to = NOW(), is_current = 0
    WHERE business_key = LEFT(v_brand,190) AND is_current = 1;

    -- Snapshot mới
    INSERT INTO `data_mart`.`agg_brand_summary`
      (business_key, `Hãng:`, product_count, min_price, max_price, avg_price,
       last_aggregated_at, effective_from, is_current)
    SELECT
      LEFT(v_brand,190),
      v_brand,
      COUNT(1),
--       MIN(CAST(`Giá` AS UNSIGNED)),
--       MAX(CAST(`Giá` AS UNSIGNED)),
--       AVG(CAST(`Giá` AS UNSIGNED)),
  MIN(NULLIF(REGEXP_REPLACE(`Giá`, '[^0-9.]', ''), '') + 0),
  MAX(NULLIF(REGEXP_REPLACE(`Giá`, '[^0-9.]', ''), '') + 0),
  AVG(NULLIF(REGEXP_REPLACE(`Giá`, '[^0-9.]', ''), '') + 0),

      NOW(), NOW(), 1
    FROM `staging`.`staging.rawtgdd`
    WHERE `Hãng:` = v_brand;
  END LOOP;

  CLOSE cur1;
  COMMIT;
END$$

DELIMITER ;




-- 9) PROCEDURE LOAD DAILY CRAWL SUMMARY


DELIMITER $$

CREATE PROCEDURE `sp_load_agg_crawl_daily`()
BEGIN
  DECLARE v_date DATE;
  DECLARE v_total INT;
  DECLARE v_distinct INT;
  DECLARE v_file TEXT;

  SET v_date = CURDATE();

  -- Tổng hợp toàn bảng staging
  SELECT COUNT(1), COUNT(DISTINCT `Source`), GROUP_CONCAT(DISTINCT `source_file` SEPARATOR '; ')
    INTO v_total, v_distinct, v_file
  FROM `staging`.`staging.rawtgdd`;

  -- Upsert (INSERT + UPDATE)
  INSERT INTO `data_mart`.`agg_crawl_daily`
    (crawl_date, source_file, total_rows, distinct_products, created_at)
  VALUES
    (v_date, v_file, v_total, v_distinct, NOW())
  ON DUPLICATE KEY UPDATE
    source_file = VALUES(source_file),
    total_rows = VALUES(total_rows),
    distinct_products = VALUES(distinct_products),
    created_at = VALUES(created_at);
END$$

DELIMITER ;
