drop table IF EXISTS SUPPLIERS
drop table IF EXISTS COFFEES
create table SUPPLIERS (SUP_ID integer NOT NULL, SUP_NAME varchar(40) NOT NULL, STREET varchar(40) NOT NULL, CITY varchar(20) NOT NULL, STATE char(2) NOT NULL, ZIP char(5), PRIMARY KEY (SUP_ID));
create table COFFEES (COF_NAME varchar(32) NOT NULL, SUP_ID int NOT NULL, PRICE numeric(10,2) NOT NULL, SALES integer NOT NULL, TOTAL integer NOT NULL, PRIMARY KEY (COF_NAME), FOREIGN KEY (SUP_ID) REFERENCES SUPPLIERS (SUP_ID));
insert into SUPPLIERS values(49, 'Superior Coffee', '1 Party Place', 'Mendocino', 'CA', '95460')
insert into SUPPLIERS values(101, 'Acme, Inc.', '99 Market Street', 'Groundsville', 'CA', '95199')
insert into SUPPLIERS values(150, 'The High Ground', '100 Coffee Lane', 'Meadows', 'CA', '93966')
insert into COFFEES values('Colombian', 00101, 7.99, 0, 0)
insert into COFFEES values('French_Roast', 00049, 8.99, 0, 0)
insert into COFFEES values('Espresso', 00150, 9.99, 0, 0)
insert into COFFEES values('Colombian_Decaf', 00101, 8.99, 0, 0)
insert into COFFEES values('French_Roast_Decaf', 00049, 9.99, 0, 0)
