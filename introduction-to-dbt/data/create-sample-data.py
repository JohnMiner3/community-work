import csv
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# Configuration
NUM_RECORDS = 25

# 1. Generate BOOKS.csv
books = []
with open('books.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['BookID', 'Title', 'Author', 'ISBN', 'Genre', 'Quantity'])
    for i in range(1, NUM_RECORDS + 1):
        book = [i, fake.catch_phrase(), fake.name(), fake.isbn13(), 
                random.choice(['Fiction', 'Sci-Fi', 'History', 'Tech', 'Biography']), 
                random.randint(1, 5)]
        books.append(book)
        writer.writerow(book)

# 2. Generate MEMBERS.csv
members = []
with open('members.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['MemberID', 'Name', 'Email', 'JoinDate', 'Status'])
    for i in range(1, NUM_RECORDS + 1):
        member = [i, fake.name(), fake.email(), 
                  fake.date_between(start_date='-3y', end_date='today'), 
                  random.choice(['Active', 'Expired'])]
        members.append(member)
        writer.writerow(member)

# 3. Generate BOOKS_ON_LOAN.csv
with open('loans.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['LoanID', 'BookID', 'MemberID', 'BorrowDate', 'DueDate', 'ReturnDate'])
    for i in range(1, NUM_RECORDS + 1):
        # Pick existing IDs from the lists above
        book_id = random.choice(books)[0]
        member_id = random.choice(members)[0]
        
        # Date logic for 2026
        borrow_date = fake.date_between(start_date='-60d', end_date='today')
        due_date = borrow_date + timedelta(days=14)
        
        # Randomly decide if it's been returned yet
        return_date = due_date - timedelta(days=random.randint(-2, 5)) if random.random() > 0.3 else ""
        
        writer.writerow([i, book_id, member_id, borrow_date, due_date, return_date])

print("Files 'books.csv', 'members.csv', and 'books_on_loan.csv' created successfully.")
