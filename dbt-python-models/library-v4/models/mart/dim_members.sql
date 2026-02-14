select  
    `MemberID`,
    `Name`,
    `Email`,
    Id as `JoinDateKey`,
    `Status`
from {{ ref('members02') }} as m
left join {{ ref('dates02') }} as d    
on m.JoinDate = d.date
where m.dbt_valid_to is NULL
  and d.dbt_valid_to is NULL