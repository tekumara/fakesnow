# timezones

duckdb:

```
select current_timestamp;                                    // 2023-06-24 17:06:31.483+10 (TIMESTAMP WITH TIME ZONE)
select current_timestamp::TIMESTAMP;                         // 2023-06-24 07:06:31.483 (TIMESTAMP)
select current_timestamp::TIMESTAMP::TIMESTAMP(9);           // 2023-06-24 07:06:31.483 (TIMESTAMP_NS)
select current_timestamp AT TIME ZONE 'UTC';                 // 2023-06-24 07:06:31.483 (TIMESTAMP)
select (current_timestamp AT TIME ZONE 'UTC')::TIMESTAMP(9); // 2023-06-24 07:06:31.483 (TIMESTAMP_NS)
select current_timestamp::TIMESTAMP(9);                      // Error: Conversion Error: Unimplemented type for cast (TIMESTAMP WITH TIME ZONE -> TIMESTAMP_NS)
```

postgres

```
select current_timestamp;                                    // 2023-06-24 07:06:31.483+00 (TIMESTAMP WITH TIME ZONE)
select current_timestamp::TIMESTAMP;                         // 2023-06-24 07:06:31.483 (TIMESTAMP WITHOUT TIME ZONE)
select current_timestamp::TIMESTAMP::TIMESTAMP(9);           // 2023-06-24 07:06:31.483 (TIMESTAMP WITHOUT TIME ZONE)
select current_timestamp AT TIME ZONE 'UTC';                 // 2023-06-24 07:06:31.483 (TIMESTAMP WITHOUT TIME ZONE)
select (current_timestamp AT TIME ZONE 'UTC')::TIMESTAMP(9); // 2023-06-24 07:06:31.483 (TIMESTAMP WITHOUT TIME ZONE)
select current_timestamp::TIMESTAMP(9);                      // 2023-06-24 07:06:31.483 (TIMESTAMP WITHOUT TIME ZONE)
```

snowflake:

```
select current_timestamp;                                    // 2023-06-24 07:06:31.483+00 (TIMESTAMP_LTZ(9))
select current_timestamp::TIMESTAMP;                         // 2023-06-24 07:06:31.483 (TIMESTAMP_NTZ(9))
```
