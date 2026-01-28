select  
    [MemberID],
    [Name],
    [Email],
    Id as [JoinDateKey],
    [Status]
from {{ ref('members01') }} as m
left join {{ ref('dates01') }} as d    
on m.JoinDate = d.date

