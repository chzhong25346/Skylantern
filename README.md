# Skylantern

Skylantern is a python script to fetch quotes (CSI300) from Tushare  and to write quotes to local database. Also it makes analysis reports that mark down important market signals and saves into tables. Lastly it simulates trading based on these signals.

## Getting Started

Python3.5 or higher version and Pandas, Numpy, SQLAlchemy and ect.
A loca/remote Mysql is required and db "tsxci", "nasdaq100" and "sp100" are created.
Environment varibles to enter in OS:
```
DB_USER="DB_USER"
DB_PASS="DB_PASSWORD"
DB_HOST="localhost"
DB_PORT="3306"
EMAIL_USER="SENDER_GMAIL"
EMAIL_PASS="SENDER_GMAIL_PWD"
EMAIL_TO="MYDOG@GMAIL.COM,MYCAT@GMAIL.COM"
AV_KEY="ALPHAVANTAGE_KEY"
```

### Prerequisites

What things you need to install the software and how to install them

```
python3.5+
pandas, numpy, sqlalchemy, yaml, logger and etc.
mysql installed separately
```


### Usage

Update quotes

```
run.py -u <full|compact|fix> <nasdaq100|tsxci|sp100>
```
*full: All quotes in all dates
compact: Recent three months
fix: fixing missing db due to connection timeout*


Reporting
```
run.py -r <nasdaq100|tsxci|sp100>
```

Simulate Trading
```
run.py -s <nasdaq100|tsxci|sp100>
```

Email out trading result of the day
```
run.py -e
```


## Authors

* **Colin Zhong** - *Initial work* - [Git Page](https://github.com/chzhong25346)
