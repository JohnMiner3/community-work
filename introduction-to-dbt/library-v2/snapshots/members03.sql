{% snapshot members03 %}

{{ config(
    strategy='check',
    unique_key='MemberID',
    check_cols=['Name', 'Email', 'JoinDate', 'Status']
) }}

select * from {{ ref('members01') }}

{% endsnapshot %}
