select 
    EmployeeId,
    LastName,
    FirstName,
    Title,
    ReportsTo,
    {{ cast_string_to_datetime2('BirthDate', 101) }} as BirthDate,
    {{ cast_string_to_datetime2('HireDate', 101) }} as HireDate,
    Address, 
    City, 
    State,
    Country,
    PostalCode,
    Phone, 
    Fax, 
    Email
from {{ source('Chinook', 'Employee') }}