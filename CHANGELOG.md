# Changelog

## [0.3.0](https://github.com/tekumara/fakesnow/compare/v0.2.0...v0.3.0) (2023-07-15)


### Features

* info schema columns returns lengths for text types ([edb91e5](https://github.com/tekumara/fakesnow/commit/edb91e56adc8026b405c12582cfce2ccfe5fc10d)), closes [#11](https://github.com/tekumara/fakesnow/issues/11)
* support ALTER TABLE .. SET COMMENT ([6470bae](https://github.com/tekumara/fakesnow/commit/6470baeafc492a451696fc828242ed4d6e88f667))
* support setting the timezone ([228e884](https://github.com/tekumara/fakesnow/commit/228e884cd0d9ef864a1bbd6080bf762a4f9ca85c))
* support timestamp_ntz(9) as a table column ([ef28927](https://github.com/tekumara/fakesnow/commit/ef2892746e958d2cd7320d55d75e1c14b927d2f8))
* treat float as 64 bit ([cce50ba](https://github.com/tekumara/fakesnow/commit/cce50bac0ad818fe76db4dbe56422fe1d8e9eea0))


### Bug Fixes

* alter table .. add column ([612e1aa](https://github.com/tekumara/fakesnow/commit/612e1aa41668d7afdbe4e0961de4fbcc853a52d1))
* fetchone() when using DictCursor ([e1e50f7](https://github.com/tekumara/fakesnow/commit/e1e50f701a2d17d042ee5207d8d71bff8df5a5fe))
* match snowflake's integer precision ([49afda2](https://github.com/tekumara/fakesnow/commit/49afda20f93fd8f2f581225060eb1e53b528c9c8)), closes [#12](https://github.com/tekumara/fakesnow/issues/12)


### Chores

* bump sqlglot 16.8.1 ([8f30d1a](https://github.com/tekumara/fakesnow/commit/8f30d1a5e4c8d1e37b86e2a73c718cb9e11a4adb))

## [0.2.0](https://github.com/tekumara/fakesnow/compare/v0.1.0...v0.2.0) (2023-06-24)


### Features

* support commit and rollback on connection ([c5520b0](https://github.com/tekumara/fakesnow/commit/c5520b08cf6df4fd0511cbe471292d121af7a469)), closes [#6](https://github.com/tekumara/fakesnow/issues/6)


### Bug Fixes

* describe to work with parameterized SQL queries ([#7](https://github.com/tekumara/fakesnow/issues/7)) ([1adaad0](https://github.com/tekumara/fakesnow/commit/1adaad01782801a364e17233dc0591990379f58b))
* No module named 'pandas' when using pdm ([f60d45d](https://github.com/tekumara/fakesnow/commit/f60d45d4526d7d2b0da57a904596389242e62f98)), closes [#5](https://github.com/tekumara/fakesnow/issues/5)

## [0.1.0](https://github.com/tekumara/fakesnow/compare/0.0.2...v0.1.0) (2023-06-20)


### Features

* cursor description ([d7d0bb3](https://github.com/tekumara/fakesnow/commit/d7d0bb30bd2c0e854ce13fa8cf6599e88587eae5))
* describe supports more column types ([b16175d](https://github.com/tekumara/fakesnow/commit/b16175da81427efbfef02c03a5790ff1826e08d0))
* support executemany ([94f17b2](https://github.com/tekumara/fakesnow/commit/94f17b28403d469d716cc9578cb2b6b1bcc603c0))
* support object_construct ([6d3e82a](https://github.com/tekumara/fakesnow/commit/6d3e82a017bf986a5ba83b21854f9fba965510f4))
* support pyformat style params ([3ba53f9](https://github.com/tekumara/fakesnow/commit/3ba53f94d6bbc547b1a4489a461c5bd1ea13c9bc))
* support write_pandas for dataframes with dict values ([ba210cd](https://github.com/tekumara/fakesnow/commit/ba210cdf66b67e7ace2edeacff400993e5f7ec5b))
* support write_pandas with partial columns ([02fd6a4](https://github.com/tekumara/fakesnow/commit/02fd6a40a05f7841ddc8ffb063cb3fb2085a71db))
* to_date supports timestamp(9) ([2263430](https://github.com/tekumara/fakesnow/commit/2263430637f06db6cb91adb7a39efd5a2055098a))


### Bug Fixes

* don't order object keys alphabetically ([cbef428](https://github.com/tekumara/fakesnow/commit/cbef4285e4d5611003386b5bb30649d589f9f3e7))
* object_construct now supports different types ([fa50752](https://github.com/tekumara/fakesnow/commit/fa50752ff36694d6e2255f26999cd3854e5541bb))
* remove docformatter ([890f1df](https://github.com/tekumara/fakesnow/commit/890f1df9b59028382afdfb8b41a3b081103413e7)), closes [#1](https://github.com/tekumara/fakesnow/issues/1)


### Documentation

* add pip install ([4472976](https://github.com/tekumara/fakesnow/commit/44729761db556237aa734b0c25b9640b5bcbe18e))
* better example of imports ([1b4c78e](https://github.com/tekumara/fakesnow/commit/1b4c78ec47d3034e6a7111bc2a0ae6fbb9979c99))
