import pandas as pd
reltn_gen=pd.read_csv("relation_generation.csv")
print(reltn_gen.head())

acct_nom=pd.read_excel('acct_nomination_table.xlsx')
print(acct_nom.head())

acct_nom[['nom_name','nom_reltn_code']]=reltn_gen[['Full Name','Relation']].values
print(acct_nom.sample(25))
acct_nom.to_csv("acct_nomination_table.csv",index=False)

