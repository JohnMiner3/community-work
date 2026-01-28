--
-- duplicate values
--

select * from [data_raw].[books01]
where BookId = 1;

insert into [data_raw].[books01]
values
(1,'Configurable stable methodology','Jeffrey Carey', '978-0-03-050997-1', 'Tech', 0)


--
-- null values
--

select * from [data_raw].[books01]
where BookId is null;

insert into [data_raw].[books01]
values
(Null,'Configurable stable methodology','Jeffrey Carey', '978-0-03-050997-1', 'Tech', 0)


--
-- accepted values
--

select * from [data_raw].[books01]
where BookId = 1;

update [data_raw].[books01]
set Genre = 'Health'
where BookId = 1;


--
-- referential test
--

select * from [data_raw].[loans01] where LoanId = 1;

update 
[data_raw].[loans01] 
set BookId = 99
where LoanId = 1;

--
--  How many loans have due dates > 30 days
--

select * 
from data_raw.loans01
where ReturnDate is null
and datediff(d, DueDate, getdate()) > 30