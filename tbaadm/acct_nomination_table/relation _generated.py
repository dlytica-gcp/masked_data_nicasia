import pandas as pd
import random

first_names_male = ['Ram', 'Shyam', 'Kiran', 'Dipak', 'Anil','Hari','Sunil','Raj','Dev','Abhaya','Prasant','Sisir','Praful','Safal']
first_names_female = ['Sita', 'Gita', 'Mina', 'Sabita', 'Sunita','Shreya','Moni','Sapana']

middle_names_male = ['Bahadur', 'Kumar', 'Prasad', 'Raj', 'Man', 'Lal', 'Singh', 'Krishna']
middle_names_female = ['Maya', 'Devi', 'Kumari', 'Laxmi', 'Rani']

surnames = ['Shrestha', 'Thapa', 'Rai', 'Gurung', 'Tamang', 'K.C.', 'Bhandari', 'Maharjan','Timalsina','Karki','Sherpa']

n = 2461

data = []
for _ in range(n):
    gender = random.choice(['Male', 'Female'])

    if gender == 'Male':
        first_name = random.choice(first_names_male)
        has_middle = random.random() < 0.7  # 70% chance to have middle name
        middle_name = random.choice(middle_names_male) if has_middle else ''
        relation = random.choice(['Father', 'Husband'])
    else:
        first_name = random.choice(first_names_female)
        has_middle = random.random() < 0.7
        middle_name = random.choice(middle_names_female) if has_middle else ''
        relation = random.choice(['Mother', 'Wife'])

    surname = random.choice(surnames)
    
    # Full name handling
    if middle_name:
        full_name = f"{first_name} {middle_name} {surname}"
    else:
        full_name = f"{first_name} {surname}"

    data.append({
        'First Name': first_name,
        'Middle Name': middle_name,
        'Surname': surname,
        'Full Name': full_name,
        'Gender': gender,
        'Relation': relation
    })
print(data)
df = pd.DataFrame(data)
print(df.head())
print(df.shape)
df.to_csv("relation_generation.csv",index=False)