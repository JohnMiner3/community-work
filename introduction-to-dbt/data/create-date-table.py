import pandas as pd
from datetime import datetime, timedelta

# Generate date range from 2023 to 2026
start_date = datetime(2023, 1, 1)
end_date = datetime(2026, 12, 31)

# Create date range
date_range = pd.date_range(start=start_date, end=end_date, freq='D')

# Create dimension table
date_dim = pd.DataFrame({
    'id': range(1, len(date_range) + 1),
    'date': date_range,
    'year': date_range.year,
    'quarter': date_range.quarter,
    'month': date_range.month,
    'month_name': date_range.strftime('%B'),
    'day': date_range.day,
    'day_of_week': date_range.dayofweek,
    'day_name': date_range.strftime('%A'),
    'week_of_year': date_range.isocalendar().week,
    'is_weekend': date_range.dayofweek.isin([5, 6]).astype(int)
})

# Save to CSV
date_dim.to_csv('dates.csv', index=False)

print(f"Date dimension table created with {len(date_dim)} rows")
print(date_dim.head())