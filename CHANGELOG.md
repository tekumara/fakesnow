<!-- markdownlint-disable MD012 MD024 -->

# Changelog

## [0.9.31](https://github.com/tekumara/fakesnow/compare/v0.9.30...v0.9.31) (2025-03-18)


### Features

* handle additional syntax errors ([4044e7a](https://github.com/tekumara/fakesnow/commit/4044e7a9badcb37c4922abaad8d76afb0ef3ef9b))
* SHOW TERSE TABLES LIKE ([58abdda](https://github.com/tekumara/fakesnow/commit/58abddaee6a8ebffcc265c79b991144b36e2c63b))
* treat number(38,0) as int ([0d8160d](https://github.com/tekumara/fakesnow/commit/0d8160d223c069890eb96e265a16430492fb75c1)), closes [#193](https://github.com/tekumara/fakesnow/issues/193)


### Chores

* bump pyarrow-stubs 17.19 ([647c032](https://github.com/tekumara/fakesnow/commit/647c03279681cc0ef5cd89994d70d95ccc2232a5))

## [0.9.30](https://github.com/tekumara/fakesnow/compare/v0.9.29...v0.9.30) (2025-03-16)


### Features

* **server:** log sql text on unhandled exception during query ([2fbb9d8](https://github.com/tekumara/fakesnow/commit/2fbb9d8636bcd924b9bb218ca5e91679a6bc7774))
* **server:** return AUTOCOMMIT parameter for JDBC driver ([89bcb69](https://github.com/tekumara/fakesnow/commit/89bcb69f11c5a32fcc5b58aa6d427fd3f6b375a3)), closes [#178](https://github.com/tekumara/fakesnow/issues/178)
* **server:** support session close ([f93f924](https://github.com/tekumara/fakesnow/commit/f93f924931253c68379b38f5164e854a9b8ecdfa)), closes [#176](https://github.com/tekumara/fakesnow/issues/176)
* show databases ([8615334](https://github.com/tekumara/fakesnow/commit/8615334de2d5b0b86c2e9e3789f33c57e4da1127)), closes [#179](https://github.com/tekumara/fakesnow/issues/179)
* show functions ([cd8f602](https://github.com/tekumara/fakesnow/commit/cd8f6026ad3e9f6a272937bc88916c33a486c191)), closes [#186](https://github.com/tekumara/fakesnow/issues/186)
* show procedures ([29b7a8d](https://github.com/tekumara/fakesnow/commit/29b7a8d7c3747f62f2195f2d55110400e9e0299c)), closes [#187](https://github.com/tekumara/fakesnow/issues/187)


### Bug Fixes

* _fs_* tables showing in dbeaver ([77def7f](https://github.com/tekumara/fakesnow/commit/77def7f4851f1d6ae5cfd2b18f14dd2df3092b70)), closes [#188](https://github.com/tekumara/fakesnow/issues/188)
* allow SHOW when not database set ([c6ec3db](https://github.com/tekumara/fakesnow/commit/c6ec3dbda8fbd9222b3d75f86c0bdc6406f4ba65))
* handle syntax errors ([53f4632](https://github.com/tekumara/fakesnow/commit/53f463201c76bf4de07fa036927a7c5fb21ee518))
* USE DATABASE/SCHEMA description ([5c208ad](https://github.com/tekumara/fakesnow/commit/5c208ad1531b1fb8ed8af922164d9cc41591dd91))


### Chores

* **deps-dev:** bump pyright from 1.1.393 to 1.1.396 ([#182](https://github.com/tekumara/fakesnow/issues/182)) ([822e98a](https://github.com/tekumara/fakesnow/commit/822e98af0660322ee35c5cfce37ad248f3dce69d))
* **deps:** update ruff requirement from ~=0.9.4 to ~=0.11.0 ([#190](https://github.com/tekumara/fakesnow/issues/190)) ([08f53df](https://github.com/tekumara/fakesnow/commit/08f53dff319547f67518be581eae2c2c7f1f3805))
* **deps:** update setuptools requirement from ~=75.6 to ~=76.0 ([#192](https://github.com/tekumara/fakesnow/issues/192)) ([6c60c34](https://github.com/tekumara/fakesnow/commit/6c60c343ad94bdb481d67203b22d199a3f7ed078))
* **deps:** update sqlglot requirement from ~=26.6.0 to ~=26.7.0 ([#180](https://github.com/tekumara/fakesnow/issues/180)) ([4668547](https://github.com/tekumara/fakesnow/commit/466854728271af0ced11b55e79c032b94a80d6cf))
* **deps:** update sqlglot requirement from ~=26.7.0 to ~=26.10.1 ([#191](https://github.com/tekumara/fakesnow/issues/191)) ([5986c9a](https://github.com/tekumara/fakesnow/commit/5986c9acd7ff4e96cfde1ec59a6e5b7fb4665f80))

## [0.9.29](https://github.com/tekumara/fakesnow/compare/v0.9.28...v0.9.29) (2025-02-16)


### Features

* **server:** support non-gzipped requests ([e24366a](https://github.com/tekumara/fakesnow/commit/e24366ae1405a58d6b2969f066e6d5e0167c9a57))
* support paramstyle as connection argument ([f7d0215](https://github.com/tekumara/fakesnow/commit/f7d02156d36c7e014362473f1e1550e2bf2546ef))


### Bug Fixes

* info schema with current database objects only ([f5b3903](https://github.com/tekumara/fakesnow/commit/f5b3903f258fe665d1ea513aff20568ddd18f324))


### Chores

* **deps:** bump duckdb 1.2.0 ([17eb5d5](https://github.com/tekumara/fakesnow/commit/17eb5d56657fc9beea93638a79c28b1f1aa2a9ae))
* **deps:** update sqlglot requirement from ~=26.3.9 to ~=26.6.0 ([#171](https://github.com/tekumara/fakesnow/issues/171)) ([74b8817](https://github.com/tekumara/fakesnow/commit/74b8817de862bfe42f23122defbc73247320f00a))

## [0.9.28](https://github.com/tekumara/fakesnow/compare/v0.9.27...v0.9.28) (2025-02-09)


### Features

* sqlid as a uuid with server support ([cd988d8](https://github.com/tekumara/fakesnow/commit/cd988d8c35ab2064219d218376fd1d65025641b0))


### Bug Fixes

* add connection autocommit ([f634311](https://github.com/tekumara/fakesnow/commit/f634311a642e0a51e3ad5161d8bb348a11a7a983))
* don't require numpy at import time ([7d89904](https://github.com/tekumara/fakesnow/commit/7d89904fe91cf2d94c79e92a55ec0ecc349be071))
* **server:** rowcount ([58e15c5](https://github.com/tekumara/fakesnow/commit/58e15c5f3e1e274164d343b511e4b10bf0ff6a9f))
* **server:** support duckdb uint64 ([7a6f9a3](https://github.com/tekumara/fakesnow/commit/7a6f9a3897e66b79b060624a400c609a7bc04595))
* support duckdb int128 description ([feffa8c](https://github.com/tekumara/fakesnow/commit/feffa8c2e87e486ef87a393dec24c76b1b9c67c4))
* support duckdb uint64 description ([62ef477](https://github.com/tekumara/fakesnow/commit/62ef477b79326611ad4664922c9aad9d6078a732))


### Chores

* **deps-dev:** bump pyright from 1.1.389 to 1.1.393 ([#167](https://github.com/tekumara/fakesnow/issues/167)) ([ccf090a](https://github.com/tekumara/fakesnow/commit/ccf090ac03fdd8ac95923b7973bf9225c0acfcff))
* **deps-dev:** bump ruff~=0.9.4 ([2d11ce6](https://github.com/tekumara/fakesnow/commit/2d11ce6919a4859e0a98facdefd12bc845084f9a))
* **deps:** bump sqlglot~=26.3.9 ([2e4583c](https://github.com/tekumara/fakesnow/commit/2e4583c9bd6704de54fb591283b1a7bbc686e9be))

## [0.9.27](https://github.com/tekumara/fakesnow/compare/v0.9.26...v0.9.27) (2024-12-08)


### Bug Fixes

* aliased column with table identifier used in join ([26794da](https://github.com/tekumara/fakesnow/commit/26794da2de361ccf60eb284309e2a814ed6b6145)), closes [#159](https://github.com/tekumara/fakesnow/issues/159)


### Chores

* **deps-dev:** bump pyright from 1.1.387 to 1.1.389 ([#158](https://github.com/tekumara/fakesnow/issues/158)) ([113c44c](https://github.com/tekumara/fakesnow/commit/113c44c5625b73bc62211a8c77d6992df4b81834))
* **deps:** update ruff requirement from ~=0.7.2 to ~=0.8.1 ([#157](https://github.com/tekumara/fakesnow/issues/157)) ([341dfed](https://github.com/tekumara/fakesnow/commit/341dfed78bd210d7d3c3346c773715b9981c4f15))
* **deps:** update setuptools requirement from ~=69.1 to ~=75.6 ([#155](https://github.com/tekumara/fakesnow/issues/155)) ([04d0a91](https://github.com/tekumara/fakesnow/commit/04d0a91db7bed425bc3d72b99759c47703b836e0))
* **deps:** update snowflake-sqlalchemy requirement from ~=1.6.1 to ~=1.7.0 ([#153](https://github.com/tekumara/fakesnow/issues/153)) ([10a4d31](https://github.com/tekumara/fakesnow/commit/10a4d31ee2a2134a9efba0cb1fcb5f6c8a8fb24e))
* **deps:** update sqlglot requirement from ~=25.24.1 to ~=25.34.0 ([#161](https://github.com/tekumara/fakesnow/issues/161)) ([58fb469](https://github.com/tekumara/fakesnow/commit/58fb469b7a60705fbc9a979b4c7df95ac4315750))
* **deps:** update twine requirement from ~=5.0 to ~=6.0 ([#156](https://github.com/tekumara/fakesnow/issues/156)) ([aef5a2b](https://github.com/tekumara/fakesnow/commit/aef5a2ba44cef0e2b602358e9e790357522b710f))

## [0.9.26](https://github.com/tekumara/fakesnow/compare/v0.9.25...v0.9.26) (2024-11-09)


### Features

* flatten returns an index column ([f603d0c](https://github.com/tekumara/fakesnow/commit/f603d0c7bf35c6c3a743bbbbde87cdd0e7654f71)), closes [#143](https://github.com/tekumara/fakesnow/issues/143)
* Support TRUNCATE TABLE description ([#144](https://github.com/tekumara/fakesnow/issues/144)) ([234bbaf](https://github.com/tekumara/fakesnow/commit/234bbaf1e4ff0f9447e3f61d07d91041ef539f45))


### Chores

* bump sqlglot 25.22.0 ([3c1f244](https://github.com/tekumara/fakesnow/commit/3c1f244144911aa106ebe5260797686fa25035f7))
* cruft update ([5d09f8b](https://github.com/tekumara/fakesnow/commit/5d09f8bdd9a256d85fe0624df1cf6c6e6f1dcbf4))
* **deps-dev:** bump pyright from 1.1.378 to 1.1.382 ([#142](https://github.com/tekumara/fakesnow/issues/142)) ([a3898ce](https://github.com/tekumara/fakesnow/commit/a3898ce3bfe17424820519f256e8e759a674ef92))
* **deps-dev:** bump pyright from 1.1.382 to 1.1.387 ([#145](https://github.com/tekumara/fakesnow/issues/145)) ([d6058d7](https://github.com/tekumara/fakesnow/commit/d6058d7c089e54f27bd807d9c8a5f6237efb57f8))
* **deps:** update duckdb requirement from ~=1.0.0 to ~=1.1.3 ([#150](https://github.com/tekumara/fakesnow/issues/150)) ([ecd4d46](https://github.com/tekumara/fakesnow/commit/ecd4d4629f2110ee2d4799b64fb3ab00ddcdf7ac))
* **deps:** update pre-commit requirement from ~=3.4 to ~=4.0 ([#147](https://github.com/tekumara/fakesnow/issues/147)) ([5a1f866](https://github.com/tekumara/fakesnow/commit/5a1f8665682c5bdc41f6695d3642bfa543f50a07))
* **deps:** update ruff requirement from ~=0.6.3 to ~=0.7.2 ([#146](https://github.com/tekumara/fakesnow/issues/146)) ([e9ae13a](https://github.com/tekumara/fakesnow/commit/e9ae13a2cb157ed32dbc4a9364bfbb1c3646c5a1))
* **deps:** update snowflake-sqlalchemy requirement from ~=1.5.0 to ~=1.6.1 ([#119](https://github.com/tekumara/fakesnow/issues/119)) ([89a315a](https://github.com/tekumara/fakesnow/commit/89a315a1b35120cd8221322fee74c075241df3d4))
* **deps:** update sqlglot requirement from ~=25.22.0 to ~=25.24.1 ([#141](https://github.com/tekumara/fakesnow/issues/141)) ([8e7c343](https://github.com/tekumara/fakesnow/commit/8e7c343fbb6bce08ce32839d38ba337001006619))

## [0.9.25](https://github.com/tekumara/fakesnow/compare/v0.9.24...v0.9.25) (2024-09-16)


### Features

* Adds MERGE INTO transform ([#109](https://github.com/tekumara/fakesnow/issues/109)) ([d5e14a7](https://github.com/tekumara/fakesnow/commit/d5e14a79b576bcd515c20704b4b3c701d68229fc))
* close duckdb connection ([223f8e2](https://github.com/tekumara/fakesnow/commit/223f8e21898cdc610d3003582e55b0cbaec9d1e7))
* **server:** handle snowflake ProgrammingError ([9455a43](https://github.com/tekumara/fakesnow/commit/9455a438d7392061dd87954a5968986aa21ea87b))
* **server:** support empty result set ([b967b69](https://github.com/tekumara/fakesnow/commit/b967b69809c0d5421caf122ad78437f39a842fd4))
* **server:** support FAKESNOW_DB_PATH ([af79f77](https://github.com/tekumara/fakesnow/commit/af79f7728a1af2396ad8d4c88a1235112185e3c8))
* **server:** support time & timestamp types ([1606a3e](https://github.com/tekumara/fakesnow/commit/1606a3e4570b24057a3e21e01d5b50e06c4e530b))
* support MERGE with multiple join columns and source subqueries  ([#136](https://github.com/tekumara/fakesnow/issues/136)) ([9b5a7a0](https://github.com/tekumara/fakesnow/commit/9b5a7a08ef2c7225f1f5324dd667b5518015026e)), closes [#24](https://github.com/tekumara/fakesnow/issues/24)


### Chores

* **deps-dev:** bump pyright from 1.1.374 to 1.1.378 ([#133](https://github.com/tekumara/fakesnow/issues/133)) ([593a420](https://github.com/tekumara/fakesnow/commit/593a4205a0364149d18bc8b1aa53a71fceacce45))
* **deps:** update ruff requirement from ~=0.5.1 to ~=0.6.3 ([#130](https://github.com/tekumara/fakesnow/issues/130)) ([6b37d8b](https://github.com/tekumara/fakesnow/commit/6b37d8bb968b9c5b2e51e8e6b76f9f517b2de532))

## [0.9.24](https://github.com/tekumara/fakesnow/compare/v0.9.23...v0.9.24) (2024-08-21)


### Bug Fixes

* don't require pandas at import time ([2a7944e](https://github.com/tekumara/fakesnow/commit/2a7944eeb371c6b2180016a84dce915449520fea)), closes [#127](https://github.com/tekumara/fakesnow/issues/127)

## [0.9.23](https://github.com/tekumara/fakesnow/compare/v0.9.22...v0.9.23) (2024-08-20)


### Features

* **server:** support bool, int, float types ([efd4942](https://github.com/tekumara/fakesnow/commit/efd4942dcb5246b96501fcb65448c4b5da5509cb))
* **server:** support cur.description ([6e9c1a5](https://github.com/tekumara/fakesnow/commit/6e9c1a582e7f95e3475ad676312cc1d9d3338386))
* support conn.is_closed() ([36dd461](https://github.com/tekumara/fakesnow/commit/36dd4612017626e044cc26032acac6a998a59d58)), closes [#125](https://github.com/tekumara/fakesnow/issues/125)

## [0.9.22](https://github.com/tekumara/fakesnow/compare/v0.9.21...v0.9.22) (2024-08-11)


### Features

* DESCRIBE VIEW ([b874fc1](https://github.com/tekumara/fakesnow/commit/b874fc1ed511c8943cc5a4c0a66628068c737167))
* describe view information_schema.* ([facc390](https://github.com/tekumara/fakesnow/commit/facc390a55c976749cba6258c457ca131ab1fcc5))


### Bug Fixes

* column types for DESCRIBE ([572eaf5](https://github.com/tekumara/fakesnow/commit/572eaf58d8b268559687185fd543da0a777e956a))
* fetchmany supports irregular sizes ([3115afd](https://github.com/tekumara/fakesnow/commit/3115afd35a66f95eed3b1e4fb92dca0660c8b709))
* log all sql executed (in debug mode) ([6faa120](https://github.com/tekumara/fakesnow/commit/6faa120b17434f405d3e221f0770c15936bb4c94))


### Chores

* **deps:** update sqlglot requirement from ~=25.5.1 to ~=25.9.0 ([#123](https://github.com/tekumara/fakesnow/issues/123)) ([da78574](https://github.com/tekumara/fakesnow/commit/da78574e682c742d42c727b8bd67cb4447e02f5d))

## [0.9.21](https://github.com/tekumara/fakesnow/compare/v0.9.20...v0.9.21) (2024-08-04)


### Features

* alter table cluster by ([9a78fc2](https://github.com/tekumara/fakesnow/commit/9a78fc2658d3da8838c93381e10b4142405fb2b6))


### Bug Fixes

* Allow connection with schema=information_schema ([#122](https://github.com/tekumara/fakesnow/issues/122)) ([51e4e68](https://github.com/tekumara/fakesnow/commit/51e4e685f7c78bd520e88ef5351d39601b81a276))
* columns returned by describe view information_schema.columns ([83c62b6](https://github.com/tekumara/fakesnow/commit/83c62b6a1fdf38a864ffee8fb40a1cef3dcb9057))
* Ensure type column in information_schema views is not null ([#121](https://github.com/tekumara/fakesnow/issues/121)) ([7340a4a](https://github.com/tekumara/fakesnow/commit/7340a4a55d0657579ec1b158a1219751c6c7c84f))
* Only set variables for SetItem expressions ([#116](https://github.com/tekumara/fakesnow/issues/116)) ([0e0711c](https://github.com/tekumara/fakesnow/commit/0e0711c821fa8487bd94a9c10d14103a3a3c71c4))


### Chores

* bump sqlglot 25.5.1 ([713d93d](https://github.com/tekumara/fakesnow/commit/713d93d06fd3fb67064fc8f67b728b1bc0152628))
* cruft update ([72b791b](https://github.com/tekumara/fakesnow/commit/72b791b186bf5147a3737733d38bc6bbb39dcb10))
* **deps-dev:** bump pyright from 1.1.369 to 1.1.374 ([#118](https://github.com/tekumara/fakesnow/issues/118)) ([17a8760](https://github.com/tekumara/fakesnow/commit/17a876004114ae6382dd7febae39598932ed1e42))

## [0.9.20](https://github.com/tekumara/fakesnow/compare/v0.9.19...v0.9.20) (2024-07-10)


### Features

* SHOW PRIMARY KEYS for table ([#114](https://github.com/tekumara/fakesnow/issues/114)) ([2c006b3](https://github.com/tekumara/fakesnow/commit/2c006b31c4f1f4e39ac6bb5d435182ec7d43e938))


### Bug Fixes

* $$  not considered a variable ([235fbc1](https://github.com/tekumara/fakesnow/commit/235fbc16081e85d309f911ae0662fc4332f14de0))
* concurrent connection write-write conflict ([96ba682](https://github.com/tekumara/fakesnow/commit/96ba6826d879fdcab6bb1bf4e0aea3a3f2d406cc))

## [0.9.19](https://github.com/tekumara/fakesnow/compare/v0.9.18...v0.9.19) (2024-07-08)


### Features

* Implements basic snowflake session variables via SET/UNSET ([#111](https://github.com/tekumara/fakesnow/issues/111)) ([7696cbd](https://github.com/tekumara/fakesnow/commit/7696cbdae629971f7f61546be4301c35dd9e8173))


### Chores

* **deps-dev:** bump pyright from 1.1.366 to 1.1.369 ([#112](https://github.com/tekumara/fakesnow/issues/112)) ([7656ab9](https://github.com/tekumara/fakesnow/commit/7656ab910034e166e2b92703bf9af5a7f7fb1668))

## [0.9.18](https://github.com/tekumara/fakesnow/compare/v0.9.17...v0.9.18) (2024-06-29)


### Bug Fixes

* execute_string ignores comments ([e6513f7](https://github.com/tekumara/fakesnow/commit/e6513f79e50af5c97634849d4be02b818ab7e796))
* Support IF NOT EXISTS in CREATE DATABASE statements ([#108](https://github.com/tekumara/fakesnow/issues/108)) ([e7f3f97](https://github.com/tekumara/fakesnow/commit/e7f3f97b26fccc7758892d004e1bfab339d9c732))

## [0.9.17](https://github.com/tekumara/fakesnow/compare/v0.9.16...v0.9.17) (2024-06-23)


### Features

* SPLIT ([28f0d98](https://github.com/tekumara/fakesnow/commit/28f0d98ef863b55964f14393b0b7417d4bd83ccc))


### Bug Fixes

* ARRAY_AGG with OVER ([6d94c61](https://github.com/tekumara/fakesnow/commit/6d94c613dfc8ecfcf268ac769a56d41d777bdebf)), closes [#92](https://github.com/tekumara/fakesnow/issues/92)
* flatten VALUE cast to varchar as raw string ([818efcc](https://github.com/tekumara/fakesnow/commit/818efccacf36fecc71bf61385c192e0f0cc596eb))
* more selective flatten_value_cast_as_varchar ([7748fbf](https://github.com/tekumara/fakesnow/commit/7748fbf10db59919759acd37d2ad9cc5f1c57349))


### Chores

* **deps:** update duckdb requirement from ~=0.10.3 to ~=1.0.0 ([#107](https://github.com/tekumara/fakesnow/issues/107)) ([8ead354](https://github.com/tekumara/fakesnow/commit/8ead354202b096559605b218358dc96a2ff4058c))
* **deps:** update sqlglot requirement from ~=24.1.0 to ~=25.3.0 ([#106](https://github.com/tekumara/fakesnow/issues/106)) ([89bee6f](https://github.com/tekumara/fakesnow/commit/89bee6f06e9adc99e8524c03e91cc173e6c2b8d3))

## [0.9.16](https://github.com/tekumara/fakesnow/compare/v0.9.15...v0.9.16) (2024-06-21)


### Bug Fixes

* CREATE DATABASE when using db_path ([#103](https://github.com/tekumara/fakesnow/issues/103)) ([1f6db72](https://github.com/tekumara/fakesnow/commit/1f6db72d25691fe484256b77ac5d1ce2d5ee72d2))

## [0.9.15](https://github.com/tekumara/fakesnow/compare/v0.9.14...v0.9.15) (2024-06-07)


### Bug Fixes

* ALTER TABLE result output ([#100](https://github.com/tekumara/fakesnow/issues/100)) ([3e0e236](https://github.com/tekumara/fakesnow/commit/3e0e236fe630e41062f9ad5edb1a889f039b617b))


### Chores

* **deps-dev:** bump pyright from 1.1.361 to 1.1.366 ([#102](https://github.com/tekumara/fakesnow/issues/102)) ([81839a4](https://github.com/tekumara/fakesnow/commit/81839a4cabf2c694c0c5eca582a469ca4cf29a53))

## [0.9.14](https://github.com/tekumara/fakesnow/compare/v0.9.13...v0.9.14) (2024-06-02)


### Features

* support alias in join ([8d31dce](https://github.com/tekumara/fakesnow/commit/8d31dcec4f8b25089fa49d8baad796836785b553)), closes [#90](https://github.com/tekumara/fakesnow/issues/90)
* support TIMESTAMP_NTZ ([0493ce6](https://github.com/tekumara/fakesnow/commit/0493ce6e068931d25c5f2fef333e135c2f0988a0)), closes [#96](https://github.com/tekumara/fakesnow/issues/96)


### Bug Fixes

* CREATE TAG is a no-op ([5085b59](https://github.com/tekumara/fakesnow/commit/5085b599b5b666b426ef727dd5536d96ccb6777c)), closes [#94](https://github.com/tekumara/fakesnow/issues/94)


### Chores

* bump duckdb 0.10.3 ([7c2a29b](https://github.com/tekumara/fakesnow/commit/7c2a29bd65c84b046e77f238921ee27ed0c90167))
* bump sqlglot 24.1.0 ([d3750be](https://github.com/tekumara/fakesnow/commit/d3750be9c81bdb93f3c8ef0ea30ed24aa2b72cfc))

## [0.9.13](https://github.com/tekumara/fakesnow/compare/v0.9.12...v0.9.13) (2024-05-08)


### Features

* nop regexes to ignore unimplemented commands ([f3783bb](https://github.com/tekumara/fakesnow/commit/f3783bbac5cdc1c38f777842cc39cd2b83ded5e8))


### Chores

* bump sqlglot 23.14.0 ([e1255c0](https://github.com/tekumara/fakesnow/commit/e1255c0322d18d88711c966ff09b2695ae52ea50))

## [0.9.12](https://github.com/tekumara/fakesnow/compare/v0.9.11...v0.9.12) (2024-05-07)


### Features

* add write_pandas support for auto_create_table param ([b78ba1d](https://github.com/tekumara/fakesnow/commit/b78ba1ddedffbac7bed74462b07a9ea2def52ed0))

## [0.9.11](https://github.com/tekumara/fakesnow/compare/v0.9.10...v0.9.11) (2024-05-07)


### Bug Fixes

* write_pandas using database and schema params ([d210c0b](https://github.com/tekumara/fakesnow/commit/d210c0b4abed024ed658a017e0bc6510f2340349))

## [0.9.10](https://github.com/tekumara/fakesnow/compare/v0.9.9...v0.9.10) (2024-05-05)


### Features

* support CREATE TABLE .. CLONE ([ad049d9](https://github.com/tekumara/fakesnow/commit/ad049d99a077cec9a843a28e0efb8c8087717545))


### Chores

* **deps-dev:** bump pyright from 1.1.355 to 1.1.361 ([#85](https://github.com/tekumara/fakesnow/issues/85)) ([adfa9f0](https://github.com/tekumara/fakesnow/commit/adfa9f0d495e1377131e6d8a64f079315abebb42))
* **deps:** update ruff requirement from ~=0.3.2 to ~=0.4.2 ([#86](https://github.com/tekumara/fakesnow/issues/86)) ([d2ec882](https://github.com/tekumara/fakesnow/commit/d2ec8827f5e250d83be07f702b5a3afb1bd5ad08))

## [0.9.9](https://github.com/tekumara/fakesnow/compare/v0.9.8...v0.9.9) (2024-05-01)


### Bug Fixes

* CREATE TABLE AS with aliases and combined fields ([dbffa01](https://github.com/tekumara/fakesnow/commit/dbffa01b3abf35f8c9ed5bfda62839756fb4526c)), closes [#82](https://github.com/tekumara/fakesnow/issues/82)
* patching in fakesnow.ipynb ([388c0ef](https://github.com/tekumara/fakesnow/commit/388c0eff80b843ad9a1a72fde58e29bad1d795a5))


### Chores

* bump sqlglot 23.12.2 ([d22facb](https://github.com/tekumara/fakesnow/commit/d22facb70d605ef7589bdbf095245504f3915482)), closes [#83](https://github.com/tekumara/fakesnow/issues/83)

## [0.9.8](https://github.com/tekumara/fakesnow/compare/v0.9.7...v0.9.8) (2024-04-19)


### Features

* support trim with numeric and variant types ([#69](https://github.com/tekumara/fakesnow/issues/69)) ([00ab619](https://github.com/tekumara/fakesnow/commit/00ab619985e50624c047a704aa85060ad7938407))


### Bug Fixes

* don't double transform cur.description sql ([76551ef](https://github.com/tekumara/fakesnow/commit/76551efabc94444de4a755fa78b58a0e2fd95c14)), closes [#61](https://github.com/tekumara/fakesnow/issues/61)
* GET_PATH precedence for JSONExtractScalar ([#78](https://github.com/tekumara/fakesnow/issues/78)) ([8a888a1](https://github.com/tekumara/fakesnow/commit/8a888a1f2b048afa7201679bf03e9689bffa4caa))
* **json extraction:** only return string when casting to varchar ([#77](https://github.com/tekumara/fakesnow/issues/77)) ([173344e](https://github.com/tekumara/fakesnow/commit/173344ec4b6358aab49fc227372edd494da1a9fc))

## [0.9.7](https://github.com/tekumara/fakesnow/compare/v0.9.6...v0.9.7) (2024-04-08)


### Features

* add TRY_PARSE_JSON ([#67](https://github.com/tekumara/fakesnow/issues/67)) ([5ebf0ba](https://github.com/tekumara/fakesnow/commit/5ebf0ba3e0fba4462862374135ee4ca4820af492))
* add TRY_TO_{DECIMAL,...} ([#68](https://github.com/tekumara/fakesnow/issues/68)) ([5903af6](https://github.com/tekumara/fakesnow/commit/5903af6aa8b3235796e669246893521f3bcff698))
* cast string literals to timestamp in dateadd ([#72](https://github.com/tekumara/fakesnow/issues/72)) ([5af0a36](https://github.com/tekumara/fakesnow/commit/5af0a3676b9c3875eb9062a9fffe2a4364dc1a4e))
* cast string literals to timestamp in datediff ([#79](https://github.com/tekumara/fakesnow/issues/79)) ([c8d7b26](https://github.com/tekumara/fakesnow/commit/c8d7b26ef960be10eca506aa86c3fd4be7b87a7a))
* mimic dateadd with date + day/week/month/year ([#71](https://github.com/tekumara/fakesnow/issues/71)) ([6a8ebe2](https://github.com/tekumara/fakesnow/commit/6a8ebe2a03a9ae0a83ef49568e7a94abb14da5fc))
* support sha2 and sha2_hex with digest size of 256 ([#63](https://github.com/tekumara/fakesnow/issues/63)) ([ce345e9](https://github.com/tekumara/fakesnow/commit/ce345e9bbda17579049d60339734024bcac1d99b))


### Bug Fixes

* remove only null key/values in object_construct ([#74](https://github.com/tekumara/fakesnow/issues/74)) ([d09edb6](https://github.com/tekumara/fakesnow/commit/d09edb63a9fcfa4d0d4b3dfac663d3ec75a0965c))
* write_pandas quotes columns when inserting dataframe ([#65](https://github.com/tekumara/fakesnow/issues/65)) ([f62ab03](https://github.com/tekumara/fakesnow/commit/f62ab034204bbe029962f60938736bbf53fea73b))


### Chores

* **deps:** bump sqlglot to 23.3.0 ([#75](https://github.com/tekumara/fakesnow/issues/75)) ([9dce794](https://github.com/tekumara/fakesnow/commit/9dce794c7df0ca714344b6a242b7bc071d84a4f9))

## [0.9.6](https://github.com/tekumara/fakesnow/compare/v0.9.5...v0.9.6) (2024-03-24)


### Features

* support sqlalchemy metadata reflection ([#55](https://github.com/tekumara/fakesnow/issues/55)) ([904156c](https://github.com/tekumara/fakesnow/commit/904156c701b6430dcb600b07e7b45c7409e932d7))


### Chores

* add ruff-format (cruft update) ([46fd572](https://github.com/tekumara/fakesnow/commit/46fd5723ee8aa46ad99047b3af4c2b9ae9c40f45))
* **deps-dev:** bump pyright from 1.1.330 to 1.1.355 ([#58](https://github.com/tekumara/fakesnow/issues/58)) ([9060a2e](https://github.com/tekumara/fakesnow/commit/9060a2ee23f40bb46420d58d6a77fbb162abf304))

## [0.9.5](https://github.com/tekumara/fakesnow/compare/v0.9.4...v0.9.5) (2024-02-23)


### Features

* SHOW PRIMARY KEYS + description for CREATE VIEW + fix information_schema.columns to work with sqlalchemy ([#52](https://github.com/tekumara/fakesnow/issues/52)) ([ff0ab13](https://github.com/tekumara/fakesnow/commit/ff0ab13587cb57b615e77a95041aeac5c54a1931))


### Bug Fixes

* GET_PATH has higher precedence than comparison operators ([91c57a5](https://github.com/tekumara/fakesnow/commit/91c57a544116279e6d1e7d385b79ef20d95420d8)), closes [#53](https://github.com/tekumara/fakesnow/issues/53)

## [0.9.4](https://github.com/tekumara/fakesnow/compare/v0.9.3...v0.9.4) (2024-02-18)


### Features

* implement information_schema.views ([#50](https://github.com/tekumara/fakesnow/issues/50)) ([c6d0bab](https://github.com/tekumara/fakesnow/commit/c6d0bab95319ed7e2ccd26bbf3dfd40eeb03dcb0))

## [0.9.3](https://github.com/tekumara/fakesnow/compare/v0.9.2...v0.9.3) (2024-02-17)


### Features

* connection close ([2715d04](https://github.com/tekumara/fakesnow/commit/2715d0451036dbf6df6b1a6a82995190c7c3d6f4)), closes [#45](https://github.com/tekumara/fakesnow/issues/45)
* support snowflake.connector.paramstyle ([524d136](https://github.com/tekumara/fakesnow/commit/524d13625f4a427bec24323a13df62a5b211e1b4)), closes [#46](https://github.com/tekumara/fakesnow/issues/46)

## [0.9.2](https://github.com/tekumara/fakesnow/compare/v0.9.1...v0.9.2) (2024-02-15)


### Features

* Implement SHOW USERS. ([#47](https://github.com/tekumara/fakesnow/issues/47)) ([1be00bc](https://github.com/tekumara/fakesnow/commit/1be00bc231e285c306b2bfbd2de23608c76ef4a5))

## [0.9.1](https://github.com/tekumara/fakesnow/compare/v0.9.0...v0.9.1) (2024-02-09)


### Features

* describe deletes ([#41](https://github.com/tekumara/fakesnow/issues/41)) ([c9d41b9](https://github.com/tekumara/fakesnow/commit/c9d41b99bc144c0eae23a2ffcc49a03e97e4d3ca))

## [0.9.0](https://github.com/tekumara/fakesnow/compare/v0.8.2...v0.9.0) (2024-02-08)


### ⚠ BREAKING CHANGES

* change to Apache License

### Features

* cursor.rowcount now returns count ([8a8264e](https://github.com/tekumara/fakesnow/commit/8a8264e22ea8c7208cd15458640d493c67d70969))
* DESCRIBE TABLE ([7d0d3a7](https://github.com/tekumara/fakesnow/commit/7d0d3a7bde08a94d2a65e54b41092d60873fd1d3))
* EQUAL_NULL ([f108a36](https://github.com/tekumara/fakesnow/commit/f108a361acbd9733b9fd182c12d41eb310179c2a))
* extract column comments ([27ff048](https://github.com/tekumara/fakesnow/commit/27ff048d0f8aa1f8e77b7e5cab87466d355ac509))
* include params in debug output ([7452756](https://github.com/tekumara/fakesnow/commit/7452756a2911a1298603192caa502f809229cce8))
* SHOW OBJECTS ([84e16e8](https://github.com/tekumara/fakesnow/commit/84e16e86f699ae1d0a94d3130152f0fea4ebf82d))
* SHOW SCHEMAS ([74cd1a7](https://github.com/tekumara/fakesnow/commit/74cd1a75193de1aa1df10d8ed0cd58bb3637a522))
* SHOW TABLES ([56f6a0b](https://github.com/tekumara/fakesnow/commit/56f6a0baafbd6a1cd4a4ea448843b242121bef53)), closes [#38](https://github.com/tekumara/fakesnow/issues/38)
* support description for TIMESTAMP WITH TIME ZONE ([40a3a1e](https://github.com/tekumara/fakesnow/commit/40a3a1ec27241adfcecd7cefa444243c86c13bf1))
* support description without db/schema + dropping current schema ([3315d99](https://github.com/tekumara/fakesnow/commit/3315d99263a79df64acb8ab5bfe99cdf4ed43021))
* support on-disk databases ([6043f3d](https://github.com/tekumara/fakesnow/commit/6043f3d61f4fcc79bbfc41538365b5ae5c4f003a))
* UPDATE returns number of rows updated ([f1187b1](https://github.com/tekumara/fakesnow/commit/f1187b122c9578063d89ae5bd48e0797bcfbb585))


### Bug Fixes

* casing in description() ([db35a5d](https://github.com/tekumara/fakesnow/commit/db35a5de752984155740709b2846f8323ab09f69))
* cli passes -m and -d to target ([83254d3](https://github.com/tekumara/fakesnow/commit/83254d30a2c5b3405c8d7a4c7167873ed20d6391))
* COMMENT ON supports raw string ([50f0253](https://github.com/tekumara/fakesnow/commit/50f02535037a7cf3dc95accd687bbb47dd6a5145))
* FAKESNOW_DEBUG=snowflake corrupting sql ([81d7e3c](https://github.com/tekumara/fakesnow/commit/81d7e3cfcf13ec2dbe9b4b9bef1c364f5e955e67))
* SHOW OBJECTS shows information_schema objects ([6318881](https://github.com/tekumara/fakesnow/commit/63188813ad4bedf9b33457ccb7f06fabdf7b5cde))
* store result set per results set ([69f00a6](https://github.com/tekumara/fakesnow/commit/69f00a60812bb6cf6da99de5df35da9500b6a214))
* TO_DATE(VARCHAR) ([c5b5d7b](https://github.com/tekumara/fakesnow/commit/c5b5d7bf09c2f7aaa736c7447b1f05e666fb496a))
* write_pandas supports dicts with different keys ([5962801](https://github.com/tekumara/fakesnow/commit/59628011bef6725f5cfc943f1ea37123f5711b34))


### Chores

* bump sqlglot 21.0.1 ([e7c6c6e](https://github.com/tekumara/fakesnow/commit/e7c6c6e2b8571b047a225810571661179c5a3ec7))
* change to Apache License ([ce2e4a5](https://github.com/tekumara/fakesnow/commit/ce2e4a575b50c2170f0d110adce3b61431bf7b7e))

## [0.8.2](https://github.com/tekumara/fakesnow/compare/v0.8.1...v0.8.2) (2024-01-05)


### Features

* FAKESNOW_DEBUG=snowflake prints original snowflake sql statement ([42db788](https://github.com/tekumara/fakesnow/commit/42db788e55acca0889d91ead2c4383da8f31ee73))
* support ARRAY_SIZE ([c35d14b](https://github.com/tekumara/fakesnow/commit/c35d14b792ea7c6467bd1b7d820db0b043430550))
* support GET_PATH on nested json ([7430067](https://github.com/tekumara/fakesnow/commit/743006707feaf14a8dcb872c36851de89e53a235))
* support IDENTIFIER ([a40ac61](https://github.com/tekumara/fakesnow/commit/a40ac612dffeb899ad0c57efc85eca8365df16d5))
* support OBJECT_CONSTRUCT_KEEP_NULL ([47168fb](https://github.com/tekumara/fakesnow/commit/47168fb1e2684a9f0c885d216fac076226bfea52))
* support RANDOM ([93c2a07](https://github.com/tekumara/fakesnow/commit/93c2a077f39b44f2de2935926b005636b098ce50))
* support ResultBatch.to_pandas() ([4bfbf45](https://github.com/tekumara/fakesnow/commit/4bfbf45580752b21edd672f255844113270e295a))
* support SAMPLE ([437343f](https://github.com/tekumara/fakesnow/commit/437343f211905b135df7f76990b4804756fcd8d5))
* support TO_TIMESTAMP_NTZ ([e351fee](https://github.com/tekumara/fakesnow/commit/e351feeebfaeea715d2b16a8b01643e39f28c4ba))


### Bug Fixes

* upper/lower converts extracted json to varchar ([44cd871](https://github.com/tekumara/fakesnow/commit/44cd8713815e0b8d365e54765d081f7c131bd40a))

## [0.8.1](https://github.com/tekumara/fakesnow/compare/v0.8.0...v0.8.1) (2024-01-03)


### Features

* add fakesnow cli ([13b28df](https://github.com/tekumara/fakesnow/commit/13b28df9239b24607c701270f42f98b26df6dd36))
* FAKESNOW_DEBUG=1 prints sql commands ([e9919b7](https://github.com/tekumara/fakesnow/commit/e9919b765e75f10deb950af569cc696b45c0675c))
* support fetchmany ([382a9bf](https://github.com/tekumara/fakesnow/commit/382a9bfa7ca461808494cc47520f841abdb3bc53))
* support FLATTEN ([0b6267f](https://github.com/tekumara/fakesnow/commit/0b6267fbe33006052b3baf4f7e353f2529d1b6fd))


### Bug Fixes

* description for CREATE DATABASE ([6015ac8](https://github.com/tekumara/fakesnow/commit/6015ac81099662aca38e791aa90e10cfb3801ba9))
* description for CREATE SCHEMA ([f9b1a4d](https://github.com/tekumara/fakesnow/commit/f9b1a4d69a24c2ddc3006405ced8f831359eb94f))
* description for CREATE TABLE ([8e8374a](https://github.com/tekumara/fakesnow/commit/8e8374acf0a869e4a0914a7d8a7df3de120ee06d))
* description for DROP statements ([a3da822](https://github.com/tekumara/fakesnow/commit/a3da822f05fc7c824ddb448a71f0ce620d4d1ff4))
* description for INSERT ([ef93ad3](https://github.com/tekumara/fakesnow/commit/ef93ad3e8506a0e4436a462498f29e327ae0c8b6))
* flatten order matches array ([fc3d24f](https://github.com/tekumara/fakesnow/commit/fc3d24fe4a3ef5f988b519cd91f6acc258c466ef))
* handle commit without transaction ([d939b43](https://github.com/tekumara/fakesnow/commit/d939b435840361535f4fac84cd27d1c6f287e91d))
* handle rollback outside transaction ([af0d7ab](https://github.com/tekumara/fakesnow/commit/af0d7ab0b702731a793f85ba8e2fd82c3a456491))
* notebook no active connection ([d60f5db](https://github.com/tekumara/fakesnow/commit/d60f5db7f4dee8f484f572c4786fdd98c194ec4f))


### Chores

* add notebooks used for development ([3055fd4](https://github.com/tekumara/fakesnow/commit/3055fd4a8f6010fcca20c9af8f4941e496119016))
* bump sqlglot 20.4.0 ([2b216f9](https://github.com/tekumara/fakesnow/commit/2b216f9c2b0eaf0c160eab3a10936e04b35768b1))
* bump sqlglot 20.5.0 ([6963493](https://github.com/tekumara/fakesnow/commit/69634939dba25f5cce922a7eecc91ccbf22324f3))

## [0.8.0](https://github.com/tekumara/fakesnow/compare/v0.7.1...v0.8.0) (2023-12-28)


### ⚠ BREAKING CHANGES

* support arrays

### Features

* support arrays ([f12ddf4](https://github.com/tekumara/fakesnow/commit/f12ddf44aafba051c831c284c190a36210342922)), closes [#19](https://github.com/tekumara/fakesnow/issues/19) [#20](https://github.com/tekumara/fakesnow/issues/20) [#21](https://github.com/tekumara/fakesnow/issues/21)


### Chores

* change versioning strategy ([#29](https://github.com/tekumara/fakesnow/issues/29)) ([858e4c3](https://github.com/tekumara/fakesnow/commit/858e4c3ba850b0763e8a129bbb66a2f510c109bf))

## [0.7.1](https://github.com/tekumara/fakesnow/compare/v0.7.0...v0.7.1) (2023-12-27)


### Bug Fixes

* No module named 'pandas.core.arrays.arrow.dtype' ([6ed1d2a](https://github.com/tekumara/fakesnow/commit/6ed1d2aad7ff193f2fe24168f303b9f22de3842e))


### Chores

* bump sqlglot 19.5.1 ([9177a7f](https://github.com/tekumara/fakesnow/commit/9177a7f1f2a20c87da204bb5278cc5a909022ec6))
* cruft update ([bf6f7e1](https://github.com/tekumara/fakesnow/commit/bf6f7e12ed3816efe1e0b1280fd5356a735956f3))

## [0.7.0](https://github.com/tekumara/fakesnow/compare/v0.6.0...v0.7.0) (2023-09-09)


### Features

* add information_schema.databases ([6953d00](https://github.com/tekumara/fakesnow/commit/6953d0033c489a48374ef7add2510444a1739ad2)), closes [#22](https://github.com/tekumara/fakesnow/issues/22)

## [0.6.0](https://github.com/tekumara/fakesnow/compare/v0.5.1...v0.6.0) (2023-08-19)


### Features

* dictionary params ([01ce713](https://github.com/tekumara/fakesnow/commit/01ce7135a69c40d539e5430387f6323ca03f5041))
* support describe and info schema for ARRAY and OBJECT ([0826d1c](https://github.com/tekumara/fakesnow/commit/0826d1c988969b3116ad1172d99705518b368182))
* to_decimal, to_number, to_numeric ([8c19a8b](https://github.com/tekumara/fakesnow/commit/8c19a8bda8d9a4f1380c8b593af5c5af4d7fd280))


### Chores

* better doc strings ([53f798e](https://github.com/tekumara/fakesnow/commit/53f798ee2f2db68b0acdfb4e8d8130611bd4956a))

## [0.5.1](https://github.com/tekumara/fakesnow/compare/v0.5.0...v0.5.1) (2023-07-24)


### Bug Fixes

* describe on INTEGER column type in info schema ([652525d](https://github.com/tekumara/fakesnow/commit/652525d6e46fca6624754bb31a6d88c1ae52c6d5)), closes [#16](https://github.com/tekumara/fakesnow/issues/16)

## [0.5.0](https://github.com/tekumara/fakesnow/compare/v0.4.1...v0.5.0) (2023-07-23)


### Features

* cursor sqlstate ([04aa92e](https://github.com/tekumara/fakesnow/commit/04aa92e2e828c50471bd5bb8bb1c97c64110d227))
* sqlid ([18985e9](https://github.com/tekumara/fakesnow/commit/18985e90441c9ab7d99acbd0778e8b001d339073))
* support BINARY type in description and info schema ([32d5952](https://github.com/tekumara/fakesnow/commit/32d5952fb5fb2078d7822592ea0a7a896f457847))
* support TIME in description & TIMESTAMP in info schema ([7014d8d](https://github.com/tekumara/fakesnow/commit/7014d8dbe8f5c424a74bb62f8fd5a1d46edd2347))


### Bug Fixes

* info schema now returns FLOAT types correctly ([55d84c1](https://github.com/tekumara/fakesnow/commit/55d84c1023622b200980a673a7ec9524114e0fe1))


### Chores

* add test coverage for BOOLEAN in info schema ([9c4f254](https://github.com/tekumara/fakesnow/commit/9c4f2540a005252a6ad0c1cfd37c0a29176ea7b9))

## [0.4.1](https://github.com/tekumara/fakesnow/compare/v0.4.0...v0.4.1) (2023-07-16)


### Bug Fixes

* remove stray debugging print ([0ecc853](https://github.com/tekumara/fakesnow/commit/0ecc853608afabcf33c4f17c8297a76683e423c1))

## [0.4.0](https://github.com/tekumara/fakesnow/compare/v0.3.0...v0.4.0) (2023-07-16)


### Features

* support indices on variants ([8269552](https://github.com/tekumara/fakesnow/commit/826955239516927abf01d48c486ba8359ddb630a))
* support regexp_substr ([98abc33](https://github.com/tekumara/fakesnow/commit/98abc33a13ed7f8a60d746811089aa558ae4a243))
* very basic rowcount support ([d1116d8](https://github.com/tekumara/fakesnow/commit/d1116d8d96e2f06ff5379ce2d67380866bd40a35))


### Bug Fixes

* dataframe integer columns are int64 ([13d36df](https://github.com/tekumara/fakesnow/commit/13d36dfc81ac197590d2ece276348e6abf0bd7df))
* info schema now returns NUMBER for numeric types ([b108110](https://github.com/tekumara/fakesnow/commit/b108110b0a1e96842c7e99d37ea72fc495ac5630))
* info schema now returns TEXT for text types ([3ea4f5f](https://github.com/tekumara/fakesnow/commit/3ea4f5fc9aa04790048ac504b1fe04e9830aeba0))
* support parse_json on values columns ([d5198b8](https://github.com/tekumara/fakesnow/commit/d5198b828aa081add7a50ec8bfdab42bca17312b))


### Chores

* remove unneeded type ignore ([5214ff9](https://github.com/tekumara/fakesnow/commit/5214ff94a8c7f5c572c29e72196c3d5029ac3b1d))

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
