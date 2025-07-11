import smtplib
import os
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login("datadev2402@gmail.com", "ryeigmcbiupecfzu")
server.sendmail("datadev2402@gmail.com", "srkim@zenithcloud.com", "Subject: Test\n\nHello from Airflow.")
server.quit()

print("ok")