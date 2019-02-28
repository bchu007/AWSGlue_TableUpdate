# AWSGlue_TableUpdate

## Abstract
An automatic way to rename column headers in AWS glue giving a header file without overwriting any metadata in the current table.

## Instructions
1. Create a file in the same directory as AWSGlue_TableUpdate.py
2. Insert the column names in correct order
3. change the variables 'database', 'tablename', and 'headfile' 
  - database: the name of the AWS database
  - tablename: the name of the AWS database table
  - headfile: the name of the file created in (step 1) including extensions
4. run 

## Dependencies
- boto3
- python3.7
