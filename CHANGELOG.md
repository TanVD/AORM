# Changelog
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## 1.1.14 - 30 September 2022
- Different aggregated functions added: min, sum, avg, any, anyLast, anyHeavy 

## 1.1.13 - 20 September 2022                             
- Update kotlin to `1.7.10`
- Update jdbc driver to `0.3.2-patch9`
- Update joda-time to `2.11.1`
- Update slf4j-api to `1.7.36`
- Update kosogor to `1.0.15`

## 1.1.12 - 20 September 2022
### Updated
- ILIKE operator support has been added

## 1.1.11 - 30 July 2022
### Updated
- Low level plain text SQL queries execution support
- Fixed DbDateTime to have getValue symmetric to setValue
- Updated ClickHouse image version

## 1.1.10 - 25 April 2022
### Updated
- Update kotlin to `1.5.32`
- Update jdbc driver to `0.3.2-patch8`
- Bumped versions of other dependencies
- Change build target to JDK 11

## 1.1.9 - 7 October 2021
### Updated
- Update kotlin version to `1.5.31`
- Update jdbc driver to `0.3.1-patch`

## 1.1.8 - 19 February 2021
### Updated
- Update kotlin version to `1.4.30`
- Bumped versions of other dependencies

## 1.1.7 - 23 October 2020
### Updated
- Update kotlin version to `1.4.10`
### Added
- Added support of decimal64 and arrayDecimal64 types

## 1.1.6 - 20 August 2020
### Updated
- Update kotlin version to `1.4.0`
- Update jdbc driver to `0.2.4`

## 1.1.5 - 26 June 2019
### Updated
- Support of Float types
- Support of Distinct
- Support of Max function and Divide operator

## 1.1.4 - 7 May 2019
### Updated
- Update kotlin version to `1.3.31`
- Rollback JDBC driver to `0.1.50`
- Migrate to CircleCI, Junit and TestContainers

## 1.1.3 - 4 Apr 2019
### Updated
- Update kotlin version to `1.3.21`
- Update jdbc driver to `0.1.52`
### Added
- Added support of extended syntax of table creation (issue #7)
- Added support of lazyInsert (asynchronous batching of inserts)

## 1.1.2 - 12 Nov 2018
### Updated
- Update kotlin version to `1.3.0`

## 1.1.1 - 10 Oct 2018
### Added
- **lazyInsert functionality**. Now you can create InsertWorker object and use it for lazy batch inserts. If InsertWorker added to ConnectionContext lazyInsert call will add record to it's queue. InsertWorker will aggregate inserts by databases and tables (it has own thread for this purpose) and flush them every *n* seconds (issue #3)
- between operator for primitive types (issue #4)

### Fixed
- inList now will not produce SqlException when used with emptyList(). Now it will render in `false` in SQL (issue #5)
- SelectRow get operator now is not nullable by default. Use getOrNull to get value of null from SelectRow (issue #6)
