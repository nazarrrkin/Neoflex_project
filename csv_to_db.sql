/*COPY DS.FT_BALANCE_F(on_date, account_rk, currency_rk, balance_out)
FROM 'D:\Applications\VSCode_projects\Neoflex_project\project_csv\ft_balance_f.csv'
DELIMITER ';'
CSV HEADER;

COPY DS.FT_POSTING_F(oper_date, credit_account_rk, debet_account_rk, credit_amount, debet_amount)
FROM 'D:\Applications\VSCode_projects\Neoflex_project\project_csv\ft_posting_f.csv'
DELIMITER ';'
CSV HEADER;*/

COPY DS.MD_ACCOUNT_D(data_actual_date, data_actual_end_date, account_rk, account_number, char_type, currency_rk, currency_code)
FROM 'D:\Applications\VSCode_projects\Neoflex_project\project_csv\md_account_d.csv'
DELIMITER ';'
CSV HEADER;
