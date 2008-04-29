CREATE TABLE IF NOT EXISTS complaints (
   user_id BIGINT NOT NULL,
   file_id INT NOT NULL,
   reporter_id BIGINT NOT NULL,
   created_time TIMESTAMP NOT NULL DEFAULT NOW(),
   message VARCHAR(8192) NOT NULL default '',
   PRIMARY KEY(user_id, file_id, reporter_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

