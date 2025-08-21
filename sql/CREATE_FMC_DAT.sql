/* 膜カットデータ */
CREATE TABLE `FMC_DAT` (
    `FMC_ID` int AUTO_INCREMENT PRIMARY KEY,
    `FMC_CUT_DATE` datetime COMMENT 'カット日',
    `FMC_R1_INJ_DATE` datetime COMMENT 'R1注入日',
    `FMC_MONOMER` decimal(2) COMMENT 'モノマー',
    `FMC_ANNEAL_NO` decimal(2) COMMENT 'アニール№',
    `FMC_CUT_MACH_NO` decimal(2) COMMENT 'カット機№',
    `FMC_ITEM` decimal(3) COMMENT 'アイテム',
    `FMC_CUT_MENU` decimal(3) COMMENT 'カットメニュー',
    `FMC_FILM_PROC_DT` datetime COMMENT '膜加工日',
    `FMC_CR_FILM` decimal(1) COMMENT 'CR膜',
    `FMC_HEAT_PROC_DT` datetime COMMENT '熱処理日',
    `FMC_FILM_CURVE` decimal(2) COMMENT '膜カーブ',
    `FMC_COLOR` decimal(2) COMMENT '色',
    `FMC_AMPM` decimal(1) COMMENT 'AM/PM',
    `FMC_INPUT_QTY` decimal(4) COMMENT '投入数',
    `FMC_CUT_FOREIGN` decimal(4) COMMENT 'カットブツ',
    `FMC_CUT_WRINKLE` decimal(4) COMMENT 'カットシワ',
    `FMC_CUT_WAVE` decimal(4) COMMENT 'カットウエーブ',
    `FMC_CUT_ERR` decimal(4) COMMENT 'カットミス',
    `FMC_CUT_CRACK` decimal(4) COMMENT 'カットサケ',
    `FMC_CUT_SCRATCH` decimal(4) COMMENT 'カットキズ',
    `FMC_CUT_OTHERS` decimal(4) COMMENT 'カットその他',
    `FMC_GOOD_QTY` decimal(4) COMMENT '良品数',
    `FMC_WASH_WRINKLE` decimal(4) COMMENT '洗浄シワ',
    `FMC_WASH_SCRATCH` decimal(4) COMMENT '洗浄キズ',
    `FMC_WASH_FOREIGN` decimal(4) COMMENT '洗浄イブツ',
    `FMC_WASH_ACETONE` decimal(4) COMMENT '洗浄アセトン',
    `FMC_WASH_ERR` decimal(4) COMMENT '洗浄ミス',
    `FMC_WASH_CUT_ERR` decimal(4) COMMENT '洗浄カットミス',
    `FMC_WASH_OTHERS` decimal(4) COMMENT '洗浄その他',
    `FMC_PASS_QTY` decimal(4) COMMENT '合格数',
    `FMC_MONTH` decimal(2) COMMENT '月'

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='膜カットデータ';

ALTER TABLE `FMC_DAT` ADD COLUMN `FMC_AMPM` decimal(1) COMMENT 'AM/PM' AFTER `FMC_COLOR`;


