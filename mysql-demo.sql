create database if not exists test default character set utf8;
use test;

drop table if exists employees;

create table employees (
   name varchar(32),
   department varchar(32),
   salary varchar(32)
);

drop table if exists departments;

create table departments (
   name varchar(32),
   country varchar(32)
);

drop table if exists customers;

create table customers (
   name varchar(32),
   country varchar(32)
);

drop table if exists products;

create table products (
   name varchar(32),
   amount int(11),
   price varchar(32)
);

drop table if exists orders;

create table orders (
   customer varchar(32), 
   department varchar(32),
   product varchar(32),
   quantity int(11),
   created datetime NOT NULL DEFAULT '0000-00-00 00:00:00'
);

insert into employees (name, department, salary) values('Smith', 'American Fruits', '10,000 $');
insert into employees (name, department, salary) values('Jones', 'English Fruits', '6,000 £');
insert into employees (name, department, salary) values('Müller', 'German Fruits', '8,000 €');
insert into employees (name, department, salary) values('Meier', 'German Fruits', '7,000 €');
insert into employees (name, department, salary) values('Schulze', 'German Fruits', '6,000 €');

insert into departments (name, country) values('American Fruits', 'us'); 
insert into departments (name, country) values('English Fruits', 'en');
insert into departments (name, country) values('German Fruits', 'de');

insert into customers (name, country) values('Big', 'us'); 
insert into customers (name, country) values('Large', 'en');
insert into customers (name, country) values('Huge', 'en');
insert into customers (name, country) values('Good', 'de');
insert into customers (name, country) values('Bad', 'de');

insert into products (name, amount, price) values('Apples', 2, '1,00$');
insert into products (name, amount, price) values('Bananas', 3, '2,00$');
insert into products (name, amount, price) values('Oranges', 5, '3,00$');

insert into orders (customer, department, product, quantity) values('Big', 'American Fruits', 'Apples', 1);
insert into orders (customer, department, product, quantity) values('Large', 'German Fruits', 'Bananas', 1);
insert into orders (customer, department, product, quantity) values('Huge', 'German Fruits', 'Oranges', 2);
insert into orders (customer, department, product, quantity, created) values('Good', 'German Fruits', 'Apples', 2, '2012-06-01');
insert into orders (customer, department, product, quantity, created) values('Bad', 'English Fruits', 'Oranges', 3, '2012-06-01');
