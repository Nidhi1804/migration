# migration
Migration script to migrate db from MYSQL to MongoDB.
### Prerequisites
Nodejs 
pm2 - 'npm install -g pm2'
### Installing
```
run git clone https://github.com/Nidhi1804/migration.git
npm install 
import mysql db using ./demo_db.sql
add mongodb service url in ./appConfig.js file
run scrip using following command
pm2 start server.js --name migration

