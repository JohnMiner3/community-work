{% snapshot books02 %}

{{ config(
    strategy='check',
    unique_key='BookID',
    check_cols=['Title', 'Author', 'ISBN', 'Genre', 'Quantity']
) }}

select * from {{ ref('books01') }}

{% endsnapshot %}
