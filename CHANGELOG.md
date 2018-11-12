# Changelog
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

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