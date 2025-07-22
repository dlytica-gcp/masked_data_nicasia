import pandas as pd
df_acc=pd.read_excel(r"cards\account\account.xlsx")
df_mer_Card = pd.read_excel(r"cards\merchant\merchant.xlsx")
df_card=pd.read_excel(r"cards\card\card.xlsx")
df_charge_revol=pd.read_excel(r"cards\charge_revol_transaction\charge_revol_transaction.xlsx")
df_emi_tran=pd.read_excel(r"cards\emi_transaction\emi_transaction.xlsx")
df_gam=pd.read_excel(r"tbaadm\general_acct_mast_table\general_acct_mast_table.xlsx")
df_mob_cus=pd.read_excel(r"mobile_banking\customer\customer.xlsx")
df_mob_acc=pd.read_excel(r"mobile_banking\bank_account\bank_account.xlsx")
df_cif_mask=pd.read_csv(r"crmuser\account\cif_masked.csv")
df_acidfor=pd.read_csv(r"tbaadm\general_acct_mast_table\acid_foracid_masked.csv")
df_iso=pd.read_excel(r"mobile_banking\iso_txn_request\iso_txn_request.xlsx")
df_nfs=pd.read_excel(r"mobile_banking\nps_transaction_detail\nps_transaction_detail.xlsx")
df_mer_qr=pd.read_excel(r"qr\merchant_details\merchant_details.xlsx")
df_merch_details=pd.read_excel(r"qr\merchant_details\merchant_details.xlsx")
df_transactions_details=pd.read_excel(r"qr\transaction_details\transaction_details.xlsx")

 
 
 
 
common = set(df_merch_details['_id']) & set(df_transactions_details['merchant_id'])
 
print("Matching values:", common)
# matching = df_iso[df_iso['from_account'].isin(df_acidfor['foracid'])]
print(len(common))
 