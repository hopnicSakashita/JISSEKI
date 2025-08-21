/* 特記事項データ */
CREATE TABLE `NOTE_DAT` (
    `NOTE_ID` int AUTO_INCREMENT PRIMARY KEY,
    `NOTE_LOT_NO` varchar(17) COMMENT 'ロットNo.',
    `NOTE_DATE` datetime COMMENT '入力日',
    `NOTE_USER` decimal(4) COMMENT '入力者',
    `NOTE_TITLE` varchar(40) COMMENT '項目名',
    `NOTE_CNTNT` varchar(200) COMMENT '内容',
    `NOTE_PATH` varchar(300) COMMENT '画像パス',

    PRIMARY KEY (`NOTE_ID`),
    FOREIGN KEY (`NOTE_USER`) REFERENCES `WRK_MST`(`WRK_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='特記事項データ';