-- add all your SQL setup statements here. 

-- You can assume that the following base table has been created with data loaded for you when we test your submission 
-- (you still need to create and populate it in your instance however),
-- although you are free to insert extra ALTER COLUMN ... statements to change the column 
-- names / types if you like.

-- CREATE TABLE FLIGHTS
-- (
--  fid int NOT NULL PRIMARY KEY,
--  year int,
--  month_id int,
--  day_of_month int,
--  day_of_week_id int,
--  carrier_id varchar(3),
--  flight_num int,
--  origin_city varchar(34),
--  origin_state varchar(47),
--  dest_city varchar(34),
--  dest_state varchar(46),
--  departure_delay double precision,
--  taxi_out double precision,
--  arrival_delay double precision,
--  canceled int,
--  actual_time double precision,
--  distance double precision,
--  capacity int,
--  price double precision
--)
ALTER TABLE FLIGHTS ADD seat int CONSTRAINT CK_Seat CHECK(seat>=0);

CREATE TABLE USERS(
	username varchar(20) COLLATE Latin1_General_CS_AS NOT NULL,
	password varchar(20) COLLATE Latin1_General_CS_AS NOT NULL,
	balance int,
    PRIMARY KEY(username)
);

INSERT INTO USERS VALUES('John', 'JohnPassword', 5000);
INSERT INTO USERS VALUES('Tom', 'TOMPASSWORD', 10000);

CREATE TABLE ITINERARIES(
	Itineraries_index int,
	f1_fid int,
	f2_fid int,
	price int,
	day int,
	username varchar(20) COLLATE Latin1_General_CS_AS NOT NULL,
	FOREIGN KEY (username) references USERS(username)
);

INSERT INTO ITINERARIES VALUES(0,123,345,700,3,'John');
INSERT INTO ITINERARIES VALUES(3,345,134,800,6,'John');

CREATE TABLE RESERVATIONS(
	id int IDENTITY(1,1) PRIMARY KEY,
	username varchar(20) COLLATE Latin1_General_CS_AS NOT NULL,
	f1_fid int,
	f2_fid int,
	day int,
	pay int NOT NULL DEFAULT(0),
	price int,
	canceled int NOT NULL DEFAULT(0),
	FOREIGN KEY (username) references USERS(username),
	FOREIGN KEY (f1_fid) references FLIGHTS(fid),
);
INSERT INTO RESERVATIONS (username, f1_fid, f2_fid, day, price)
VALUES ('John', 123, 456, 3,700);
INSERT INTO RESERVATIONS (username, f1_fid, f2_fid, day, price)
VALUES ('Tom', 345, 134, 8,700);


