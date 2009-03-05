CREATE TABLE IF NOT EXISTS complaints (
   id INT,
   user_id BIGINT NOT NULL,
   file_id INT NOT NULL,
   r_id BIGINT NOT NULL,
   created_time TIMESTAMP DEFAULT NOW(),
   message VARCHAR(8192) NOT NULL default '',
   PRIMARY KEY(user_id)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS test (
   id INT NOT NULL,
   test1 VARCHAR(1000),
   blim INT
);

