import json
import pandas as pd
from datetime import datetime

CONTRACT_FILE_PATH = "./data.csv"
BANK_BLACK_LIST = ['LIZ', 'LOM', 'MKO', 'SUG', None]
OUTPUT_PATH = 'contract_features.csv'

def parse_json(json_str):
    try:
        return json.loads(json_str)
    except (TypeError, ValueError):
        return [] 
        

def calc_ft_tot_claim_cnt_l180d(contracts_json, date_start):
    valid_claims_count = 0
    application_date = datetime.strptime(date_start.split()[0], "%Y-%m-%d").date()

    for contract in contracts_json:
        if isinstance(contract, dict) and 'claim_date' in contract and contract['claim_date']:
            claim_date_str = contract['claim_date']
            try:
                claim_date = datetime.strptime(claim_date_str, "%Y-%m-%d").date()
            except ValueError:
                try:
                    claim_date = datetime.strptime(claim_date_str, "%d.%m.%Y").date()
                except ValueError:
                    continue

            if claim_date > application_date:
                claim_date = application_date
            
            if 0 <= (application_date - claim_date).days <= 180:
                valid_claims_count += 1

    return -3 if valid_claims_count == 0 else valid_claims_count


def calc_ft_disb_active_bank_loan_wo_tbc(contracts_json):
    loan_sum_total = 0
    for contract in contracts_json:
        if isinstance(contract, dict):
            bank = contract.get('bank')
            loan_sum = contract.get('loan_summa')
            contract_date = contract.get('contract_date')
            if bank not in BANK_BLACK_LIST and loan_sum and contract_date:
                loan_sum_total += float(loan_sum)
              
    return -1 if loan_sum_total == 0 else loan_sum_total


def calc_ft_day_sinlastloan(contracts_json, date_start):
    application_date = datetime.strptime(date_start.split()[0], "%Y-%m-%d").date()
    last_loan_date = None

    for contract in contracts_json:
        if isinstance(contract, dict) and 'contract_date' in contract and contract['contract_date']:
            contract_date_str = contract['contract_date']
            try:
                contract_date = datetime.strptime(contract_date_str, "%Y-%m-%d").date()
            except ValueError:
                try:
                    contract_date = datetime.strptime(contract_date_str, "%d.%m.%Y").date()
                except ValueError:
                    continue

            if contract_date <= application_date and (last_loan_date is None or contract_date > last_loan_date):
                last_loan_date = contract_date

    if last_loan_date is None:
        return -1
    days_since_last_loan = (application_date - last_loan_date).days
    return -3 if days_since_last_loan < 0 else days_since_last_loan



if __name__ == "__main__":
    data = pd.read_csv(CONTRACT_FILE_PATH)
    data['contracts_json'] = data['contracts'].apply(lambda x: parse_json(x) if pd.notnull(x) else [])

    data['tot_claim_cnt_l180d'] = data.apply(lambda row: calc_ft_tot_claim_cnt_l180d(row["contracts_json"], row["date_start"]), axis=1)
    data['disb_active_bank_loan_wo_tbc'] = data['contracts_json'].apply(calc_ft_disb_active_bank_loan_wo_tbc)
    data['day_sinlastloan'] = data.apply(lambda row: calc_ft_day_sinlastloan(row['contracts_json'], row['date_start']), axis=1)
    data.drop("contracts_json", axis=1) # Fix extra commas in output file
    data.to_csv(OUTPUT_PATH, index=False)