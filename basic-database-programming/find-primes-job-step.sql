-- Two local variables
DECLARE @VAR_ALPHA BIGINT;
DECLARE @VAR_OMEGA BIGINT;

-- Grab next min number
SELECT 
    @VAR_ALPHA = MAX(MY_VALUE) + 1 
FROM MATH.[dbo].[TBL_PRIMES];

-- Grab next max number
SELECT @VAR_OMEGA = @VAR_ALPHA + 250000;

-- Check for primes
EXEC MATH.[dbo].[STORE_PRIMES] @VAR_ALPHA, @VAR_OMEGA;