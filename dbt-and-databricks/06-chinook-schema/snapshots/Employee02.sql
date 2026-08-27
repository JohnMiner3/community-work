{% snapshot Employee02 %}

{{ config(
    strategy='check',
    unique_key='`EmployeeId`',
    check_cols=['`LastName`', '`FirstName`', '`Title`', '`ReportsTo`', '`BirthDate`', '`HireDate`', '`Address`', '`City`', '`State`', '`Country`', '`PostalCode`', '`Phone`', '`Fax`', '`Email`']
) }}

select * from {{ ref('Employee') }}

{% endsnapshot %}
