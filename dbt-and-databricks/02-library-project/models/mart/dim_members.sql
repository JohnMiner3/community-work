select  
    m."MemberID",
    m."Name",
    m."Email",
    d."id" as "JoinDateKey",
    m."Status"
from {{ ref('members01') }} as m
left join {{ ref('dates01') }} as d    
on m."JoinDate" = d."date"

