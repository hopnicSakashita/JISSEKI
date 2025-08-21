/* 設定マスタ */
CREATE TABLE `SET_MST` (
    `SET_ID` decimal(1) NOT NULL COMMENT 'ID',
    `SET_DRW_INT` decimal(6) COMMENT '描画間隔',
    `SET_CHS_TM` decimal(2) COMMENT '抽出期間',
    `SET_JS_RD_DT` datetime(2) COMMENT 'CSV取込時間',
    `SET_INFO_H1` varchar(40) COMMENT 'お知らせヘッダ１',
    `SET_INFO_1` varchar(200) COMMENT 'お知らせ１',
    `SET_INFO_H2` varchar(40) COMMENT 'お知らせヘッダ２',
    `SET_INFO_2` varchar(200) COMMENT 'お知らせ２',
    `SET_INFO_H3` varchar(40) COMMENT 'お知らせヘッダ３',
    `SET_INFO_3` varchar(200) COMMENT 'お知らせ３',
    PRIMARY KEY (`SET_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='設定マスタ';