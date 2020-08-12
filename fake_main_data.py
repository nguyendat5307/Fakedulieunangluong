import psycopg2 as pg
import pandas as pd
import datetime
import time

db_host = 'nangluong.iotmind.vn'
db_name = 'mind_nlmt'
db_user = 'lenk'
db_pass = 'mind@iot'

connection = pg.connect("host='"+db_host+"' dbname='"+db_name+"' user='"+db_user+"' password='"+db_pass+"'")
def fake_data():
    df_pattern = pd.DataFrame()
    df_total_energy_last = pd.DataFrame()
    # danh sach ngày để lấy mẫu
    listofsampledays = ['2020-07-04 00:00:00', '2020-07-05 00:00:00', '2020-07-07 00:00:00', '2020-08-01 00:00:00', '2020-08-02 00:00:00']

    # lấy 'day' và 'time' ngày hiện tại
    now = str(datetime.datetime.now())
    day = int(now[8:10])
    time = now[11:]

    # chọn ngẫu nhiên một ngày để lấy mẫu dữ liệu
    sampleday = listofsampledays[day%5]

    # lấy mẫu trong ngày lấy mẫu có cung 'time' với 'time' hiện tại
    from_date = sampleday[:11] + time
    to_date = str(datetime.datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S.%f") + datetime.timedelta(seconds=3))
    query = """
                    SELECT write_date, data_time, active_power, reactive_power, power_factor, delta_energy, topic, string_power, string_voltage, 
                    string_current, status_code, error_code, invt_temperature, envi_temperature, invt_design_power, dc_power_total,
                    phase_an_voltage, phase_bn_voltage, phase_cn_voltage, line_ab_voltage, line_bc_voltage, line_ca_voltage,
                    phase_an_current, phase_bn_current, phase_cn_current, line_ab_current, line_bc_current, line_ca_current
                    FROM sample_data
                    WHERE write_date BETWEEN '%s' AND '%s'
                    ORDER BY write_date LIMIT 1 
                    """ %(from_date, to_date)
    df_pattern = pd.read_sql(query, con=connection)
    if df_pattern["write_date"].count() == 1:
        # tính toán 'total_energy'
        query = """
                    SELECT total_energy
                    FROM fake_main_data
                    ORDER BY write_date DESC LIMIT 1 
                    """
        df_total_energy_last = pd.read_sql(query, con=connection)
        if df_total_energy_last["total_energy"].count() == 0:
            total_energy = 0
        total_energy = df_total_energy_last["total_energy"].values[0] + df_pattern["delta_energy"].values[0]
        # tạo ra mẫu hiện tại với dữ liệu fake từ mẫu tương đương về 'time' của ngày lấy mẫu
        df_pattern.drop(["delta_energy", "write_date", "date_time"], axis=1, inplace=True)
        df_pattern["total_energy"] = total_energy
        df_pattern["write_date"] = now
        df_pattern["date_time"] = now

        # viêt 'df_pattern' vào bảng 'fake_main_data' (phần này em không biết nên anh viết vào giúp em ạ)

while True:
    fake_data()
    time.sleep(4)

















