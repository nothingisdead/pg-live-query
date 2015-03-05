CREATE TABLE IF NOT EXISTS users (
	id SERIAL,
	first_name VARCHAR(255),
	last_name VARCHAR(255)
);

TRUNCATE users;

INSERT INTO users (first_name, last_name)
VALUES
	('John', 'Smith'),
	('Jane', 'Doe'),
	('Linus', 'Torvalds'),
	('Mahatma', 'Gandhi');

CREATE TABLE IF NOT EXISTS games (
	id SERIAL,
	name VARCHAR(255)
);

TRUNCATE games;

INSERT INTO games (name)
VALUES
	('Basketball'),
	('Football'),
	('Curling'),
	('Chinese Checkers');

CREATE TABLE IF NOT EXISTS scores (
	id SERIAL,
	user_id BIGINT,
	game_id BIGINT,
	score NUMERIC
);

TRUNCATE scores;

INSERT INTO scores (user_id, game_id, score)
VALUES
	(1, 1, 93),
	(1, 2, 27),
	(1, 3, 75.47),
	(1, 4, 3.141596),
	(2, 1, 2562),
	(2, 4, 46),
	(3, 3, 797),
	(3, 2, 99999),
	(3, 1, 11111)
