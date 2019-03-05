# SparkSQL
You are given two Account files pc_account_orig.csv and pc_account_mod.csv with same schema as
mentioned in the below box, which is having good and bad records. You have to merge these two files.
Write all good records in one Hive table and all bad records in another Hive table. Store the tables as
valid_pc_account and invalid_pc_account under your database.

1) Move the files pc_account_orig.csv and pc_account_mod.csv from Local Linux directory
/usr/share/usecase/ to your HDFS location(/user/”user_id”/curation) for
eg:/user/dbuser1/curation/

2) Create External Hive table on top of it using the below schema.

3) Write Spark program to Validate if the records have PublicId starts with “pc:” and the Service
Tier should be “Bronze or Gold or Platinum or Silver”

4) If yes save it in valid_pc_account table, else save in invalid_pc_account

BusOpsDesc STRING ,PublicID STRING ,CreateTime STRING ,LinkContacts INT ,AccountOrgType
INT,LocationAutoNumberSeq INT,UpdateTime STRING,ServiceTier STRING,PrimaryLanguage INT,ID
INT,StateBureauNum STRING ,PrimaryLocale INT,AccountStatus INT,Frozen INT,CreateUserID
INT,YearBusinessStarted INT ,IndustryCodeID INT,BeanVersion INT,Retired INT,LockingColumn
INT,OtherOrgTypeDescription STRING,UpdateUserID INT,AccountNumberDenorm INT,AccountNumber
INT,PreferredCoverageCurrency INT,PreferredSettlementCurrency INT,OriginationDate STRING
