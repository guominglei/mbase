create database x_editor;

use x_editor;

grant select, update, insert, delete on x_editor.* to 'x'@'%';

create table ugc_review_index(
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  data JSON NOT NULL,
  ugc_story_id int GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(data, '$.ugc_story_id'))) VIRTUAL,
  user_id BIGINT GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(data, '$.user_id'))) VIRTUAL,
  status BIGINT GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(data, '$.status'))) VIRTUAL,
  PRIMARY KEY(id),
  UNIQUE udx_user_story(user_id, ugc_story_id),
  KEY idx_story_status(ugc_story_id, status),
  KEY idx_user_status(user_id, status)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4 default charset utf8;
