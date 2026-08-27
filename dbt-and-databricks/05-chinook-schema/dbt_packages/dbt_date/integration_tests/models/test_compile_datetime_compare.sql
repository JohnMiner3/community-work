{# Frozen reference so assertions do not depend on wall-clock run_started_at. #}
{% set as_of = "2026-08-15T12:00:00Z" %}
{% set sept1 = "2026-09-01" %}
{% set sept1_utc = "2026-09-01T00:00:00Z" %}
{% set date_obj = dbt_date.date(2026, 9, 1) %}
{% set datetime_obj = dbt_date.datetime(2026, 9, 1, 0, 0, 0, tz="UTC") %}

select
    {{ 1 if dbt_date.is_before(sept1, reference=as_of) else 0 }} as before_future_date,
    {{ 1 if dbt_date.is_before("2026-08-01", reference=as_of) else 0 }}
    as before_past_date,
    {{ 1 if dbt_date.is_before(sept1_utc, reference="2026-08-31T23:59:59Z") else 0 }}
    as before_just_before_boundary,
    {{ 1 if dbt_date.is_before(sept1, reference=sept1_utc, tz="UTC") else 0 }}
    as before_at_boundary,
    {{ 1 if dbt_date.is_before_or_equal(sept1, reference=sept1_utc, tz="UTC") else 0 }}
    as before_or_equal_at_boundary,
    {{ 1 if dbt_date.is_after("2026-08-01", reference=as_of) else 0 }}
    as after_past_date,
    {{ 1 if dbt_date.is_after(sept1, reference=as_of) else 0 }} as after_future_date,
    {{ 1 if dbt_date.is_after(sept1, reference=sept1_utc, tz="UTC") else 0 }}
    as after_at_boundary,
    {{ 1 if dbt_date.is_after_or_equal(sept1, reference=sept1_utc, tz="UTC") else 0 }}
    as after_or_equal_at_boundary,
    {{ 1 if dbt_date.is_between("2026-08-01", sept1, reference=as_of) else 0 }}
    as between_inside,
    {{
        (
            1
            if dbt_date.is_between(
                "2026-08-01", sept1, reference="2026-08-01T00:00:00Z"
            )
            else 0
        )
    }} as between_at_start,
    {{ 1 if dbt_date.is_between("2026-08-01", sept1, reference=sept1_utc) else 0 }}
    as between_at_end,
    {{
        (
            1
            if dbt_date.is_between(
                "2026-08-01", sept1, reference=sept1_utc, inclusive="both"
            )
            else 0
        )
    }} as between_at_end_inclusive_both,
    {{
        (
            1
            if dbt_date.is_between(
                "2026-08-01",
                sept1,
                reference="2026-08-01T00:00:00Z",
                inclusive="neither",
            )
            else 0
        )
    }} as between_at_start_inclusive_neither,
    {{
        (
            1
            if dbt_date.is_between(
                "2026-08-01", sept1, reference=sept1_utc, inclusive="right"
            )
            else 0
        )
    }} as between_at_end_inclusive_right,
    {{
        (
            1
            if dbt_date.is_before(
                sept1, reference="2026-08-31T14:59:59Z", tz="Asia/Tokyo"
            )
            else 0
        )
    }} as before_tokyo_just_before_midnight,
    {{
        (
            1
            if dbt_date.is_before(
                sept1, reference="2026-08-31T15:00:00Z", tz="Asia/Tokyo"
            )
            else 0
        )
    }} as before_tokyo_at_midnight,
    {{
        (
            1
            if dbt_date.is_before(
                sept1, reference="2026-09-01T06:59:59Z", tz="America/Los_Angeles"
            )
            else 0
        )
    }} as before_la_just_before_midnight,
    {{
        (
            1
            if dbt_date.is_before(
                sept1, reference="2026-09-01T07:00:00Z", tz="America/Los_Angeles"
            )
            else 0
        )
    }} as before_la_at_midnight,
    {{
        (
            1
            if dbt_date.is_before(
                "2026-09-01T00:00:00Z",
                reference="2026-08-31T23:59:59Z",
                tz="Asia/Tokyo",
            )
            else 0
        )
    }} as before_aware_timestamp_ignores_tz,
    {{ 1 if dbt_date.is_before("2026-09-01 00:00:00", reference=as_of) else 0 }}
    as before_naive_space_separator,
    {{ 1 if dbt_date.is_before(date_obj, reference=as_of, tz="UTC") else 0 }}
    as before_date_object,
    {{ 1 if dbt_date.is_before(datetime_obj, reference=as_of) else 0 }}
    as before_datetime_object,
    {{ 1 if dbt_date.is_before("9999-12-31") else 0 }}
    as before_far_future_run_started_at,
    {{ 1 if dbt_date.is_after("1970-01-01") else 0 }} as after_epoch_run_started_at,
    {{ 1 if dbt_date.is_before("1970-01-01") else 0 }} as before_epoch_run_started_at,
    {{ 1 if dbt_date.is_after("9999-12-31") else 0 }}
    as after_far_future_run_started_at,
    {% if dbt_date.is_before(sept1, reference=as_of) %} 1
    {% else %} 0
    {% endif %} as if_block_before_future
