# linklion2
Second version of LinkLion

DataBase type: MySQL

DataBase name: linklion2

username: root

password: sameas


Structure of DB: https://github.com/firmao/linklion2/blob/master/DB_Tables_StoredProcedure.sql

Main class: https://github.com/firmao/linklion2/blob/master/FirstOptimization.java

JDK version 1.8
External java libraries: Jena and Java mail.

Database: https://doi.org/10.6084/m9.figshare.5005241.v1

Restore Database:

mysql -u root -p linklion2 < file.sql

Backup Database:

mysqldump -u root -p linklion2 > file.sql

File with examples of URIs belonging to more than one data-set: https://github.com/firmao/linklion2/blob/master/data.csv

# License

licensed under Apache 2.0
