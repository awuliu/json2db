

ALTER TABLE `law_judge`
ADD COLUMN `sourcedoc`  longtext NULL COMMENT '来源' AFTER `attach_file`,
ADD COLUMN `sourcetype`  varchar(255) NULL AFTER `sourcedoc`
ADD COLUMN `sourceid`  varchar(255) NULL AFTER `sourcetype`;

ALTER TABLE `law_judge`
MODIFY COLUMN `fair`  longtext CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT '裁判要旨' AFTER `cause`,
MODIFY COLUMN `result`  longtext CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT '裁判结果' AFTER `level`;



