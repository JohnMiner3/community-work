select  
    [LoanID],
    [BookID],
    [MemberID],
    d1.id as [BorrowDateKey],
    d2.id as [DueDateKey],
    d3.id as [ReturnDateKey]

from {{ ref('loans01') }} as l

left join {{ ref('dates01') }} as d1   
on l.BorrowDate = d1.date

left join {{ ref('dates01') }} as d2   
on l.DueDate = d2.date

left join {{ ref('dates01') }} as d3   
on l.ReturnDate = d3.date
