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

Database: https://www.dropbox.com/s/2m3hibbry6c51w5/backup_linklion2.sql?dl=0

Restore Database:

mysql -u root -p linklion2 < backup_linklion2.sql

Backup Database:

mysqldump -u root -p linklion2 > backup_linklion2.sql
