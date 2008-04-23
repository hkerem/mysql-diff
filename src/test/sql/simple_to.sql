CREATE TABLE IF NOT EXISTS complaints (
    id INT (11) AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    file_id INT NOT NULL,
    created_time TIMESTAMP DEFAULT NOW(),
    message VARCHAR(8192) NOT NULL default '',
    PRIMARY KEY(user_id, file_id, reporter_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

