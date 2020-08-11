import psycopg2 as pg
import pandas as pd
import datetime

db_host = 'nangluong.iotmind.vn'
db_name = 'mind_nlmt'
db_user = 'lenk'
db_pass = 'mind@iot'

connection = pg.connect("host='"+db_host+"' dbname='"+db_name+"' user='"+db_user+"' password='"+db_pass+"'")
# danh sach ngày để lấy mẫu
listofsampledays = ['2020-07-04 ', '2020-07-05 ', '2020-07-07 ', '2020-08-01 ', '2020-08-02 ']
df_sample_data = pd.DataFrame()
for d in listofsampledays:
    df = pd.DataFrame()
    from_date = d + '00:00:00'
    to_date = d + '23:59:59'
    query = """
                SELECT write_date, data_time, active_power, reactive_power, power_factor, total_energy, topic, string_power, string_voltage, 
                string_current, status_code, error_code, invt_temperature, envi_temperature, invt_design_power, dc_power_total,
                phase_an_voltage, phase_bn_voltage, phase_cn_voltage, line_ab_voltage, line_bc_voltage, line_ca_voltage,
                phase_an_current, phase_bn_current, phase_cn_current, line_ab_current, line_bc_current, line_ca_current
                FROM main_data
                WHERE write_date BETWEEN '%s' AND '%s'
                ORDER BY write_date LIMIT 1 
                """ %(from_date, to_date)
    df = pd.read_sql(query, con=connection)
    df["delta_energy"] = df["total_energy"] - df["total_energy"].shift(1).fillna(0) 
    df.drop("total_energy", axis=1, inplace=True)
    df_sample_data = df_sample_data.append(df)

# viết df_sample_data vào bảng sample_data (phần này em không biết nên anh viết vào giúp em ạ)