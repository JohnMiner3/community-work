{#
  Compile-time datetime helpers. These return Python objects / booleans during
  Jinja evaluation; they do not generate SQL.
#}
{% macro to_compile_datetime(value, tz=None) %}
    {{ return(adapter.dispatch("to_compile_datetime", "dbt_date")(value, tz)) }}
{% endmacro %}

{% macro default__to_compile_datetime(value, tz=None) %}
    {% set tz_name = tz if tz else "UTC" %}
    {% set tzinfo = modules.pytz.timezone(tz_name) %}

    {% if value is none %}
        {{
            exceptions.raise_compiler_error(
                "dbt_date.to_compile_datetime() expected a date or timestamp, got none."
            )
        }}
    {% endif %}

    {% if value is string %}
        {% set trimmed = value | trim %}
        {% if trimmed == "" %}
            {{
                exceptions.raise_compiler_error(
                    "dbt_date.to_compile_datetime() expected a date or timestamp, got an empty string."
                )
            }}
        {% endif %}
        {% if trimmed | length == 10 and trimmed[4] == "-" and trimmed[7] == "-" %}
            {% set parsed = modules.datetime.datetime.strptime(trimmed, "%Y-%m-%d") %}
            {{ return(tzinfo.localize(parsed)) }}
        {% endif %}
        {% set normalized = trimmed | replace("Z", "+00:00") | replace("z", "+00:00") %}
        {% set parsed = modules.datetime.datetime.fromisoformat(normalized) %}
        {% if parsed.tzinfo is none %} {{ return(tzinfo.localize(parsed)) }} {% endif %}
        {{ return(parsed) }}
    {% endif %}

    {# datetime-like (including run_started_at) #}
    {% if value.hour is defined %}
        {% if value.tzinfo is none %} {{ return(tzinfo.localize(value)) }} {% endif %}
        {{ return(value) }}
    {% endif %}

    {# date-like: midnight in the target timezone #}
    {% if value.year is defined %}
        {% set midnight = modules.datetime.datetime(
            value.year, value.month, value.day
        ) %}
        {{ return(tzinfo.localize(midnight)) }}
    {% endif %}

    {{
        exceptions.raise_compiler_error(
            "dbt_date.to_compile_datetime() could not interpret "
            ~ value
            ~ " as a date or timestamp."
        )
    }}
{% endmacro %}

{% macro compile_reference_datetime(reference=None, tz=None) %}
    {{
        return(
            adapter.dispatch("compile_reference_datetime", "dbt_date")(reference, tz)
        )
    }}
{% endmacro %}

{% macro default__compile_reference_datetime(reference, tz=None) %}
    {% if reference is none %}
        {% if run_started_at is not defined or run_started_at is none %}
            {{
                exceptions.raise_compiler_error(
                    "dbt_date comparison macros need a reference datetime. Pass reference= or use a dbt command that sets run_started_at."
                )
            }}
        {% endif %}
        {{ return(dbt_date.to_compile_datetime(run_started_at, tz)) }}
    {% endif %}
    {{ return(dbt_date.to_compile_datetime(reference, tz)) }}
{% endmacro %}

{% macro is_before(value, reference=None, tz=None) %}
    {{ return(adapter.dispatch("is_before", "dbt_date")(value, reference, tz)) }}
{% endmacro %}

{% macro default__is_before(value, reference=None, tz=None) %}
    {{
        return(
            dbt_date.compile_reference_datetime(reference, tz).timestamp()
            < dbt_date.to_compile_datetime(value, tz).timestamp()
        )
    }}
{% endmacro %}

{% macro is_after(value, reference=None, tz=None) %}
    {{ return(adapter.dispatch("is_after", "dbt_date")(value, reference, tz)) }}
{% endmacro %}

{% macro default__is_after(value, reference=None, tz=None) %}
    {{
        return(
            dbt_date.compile_reference_datetime(reference, tz).timestamp()
            > dbt_date.to_compile_datetime(value, tz).timestamp()
        )
    }}
{% endmacro %}

{% macro is_before_or_equal(value, reference=None, tz=None) %}
    {{
        return(
            adapter.dispatch("is_before_or_equal", "dbt_date")(value, reference, tz)
        )
    }}
{% endmacro %}

{% macro default__is_before_or_equal(value, reference=None, tz=None) %}
    {{
        return(
            dbt_date.compile_reference_datetime(reference, tz).timestamp()
            <= dbt_date.to_compile_datetime(value, tz).timestamp()
        )
    }}
{% endmacro %}

{% macro is_after_or_equal(value, reference=None, tz=None) %}
    {{
        return(
            adapter.dispatch("is_after_or_equal", "dbt_date")(value, reference, tz)
        )
    }}
{% endmacro %}

{% macro default__is_after_or_equal(value, reference=None, tz=None) %}
    {{
        return(
            dbt_date.compile_reference_datetime(reference, tz).timestamp()
            >= dbt_date.to_compile_datetime(value, tz).timestamp()
        )
    }}
{% endmacro %}

{% macro is_between(start, end, reference=None, tz=None, inclusive="left") %}
    {{
        return(
            adapter.dispatch("is_between", "dbt_date")(
                start, end, reference, tz, inclusive
            )
        )
    }}
{% endmacro %}

{% macro default__is_between(start, end, reference=None, tz=None, inclusive="left") %}
    {% set ref_dt = dbt_date.compile_reference_datetime(reference, tz).timestamp() %}
    {% set start_dt = dbt_date.to_compile_datetime(start, tz).timestamp() %}
    {% set end_dt = dbt_date.to_compile_datetime(end, tz).timestamp() %}
    {% if start_dt > end_dt %}
        {{
            exceptions.raise_compiler_error(
                "dbt_date.is_between() requires start to be less than or equal to end."
            )
        }}
    {% endif %}
    {% if inclusive == "left" %} {{ return(start_dt <= ref_dt and ref_dt < end_dt) }}
    {% elif inclusive == "right" %} {{ return(start_dt < ref_dt and ref_dt <= end_dt) }}
    {% elif inclusive == "both" %} {{ return(start_dt <= ref_dt and ref_dt <= end_dt) }}
    {% elif inclusive == "neither" %}
        {{ return(start_dt < ref_dt and ref_dt < end_dt) }}
    {% else %}
        {{
            exceptions.raise_compiler_error(
                "dbt_date.is_between() inclusive must be one of: left, right, both, neither. Got '"
                ~ inclusive
                ~ "'."
            )
        }}
    {% endif %}
{% endmacro %}
