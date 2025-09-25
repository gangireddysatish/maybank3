from pyhive import hive
import os
import traceback

# Set Kerberos environment variable for keytab
os.environ["KRB5_CLIENT_KTNAME"] = r"C:\ProgramData\MIT\Kerberos5\80013010-cdp.keytab"

# Connection parameters extracted from JDBC URL
host = "mbbbdauatdb001.mbbuat.local"
port = 10015
database = "cdptesting"
kerberos_service_name = "hive"

# === Logging and file existence checks ===
print(f"Host: {host}")
print(f"Port: {port}")
print(f"Database: {database}")
print(f"Kerberos service: {kerberos_service_name}")
print(f"Keytab: {os.environ['KRB5_CLIENT_KTNAME']}")

# Check keytab file existence
required_files = [os.environ["KRB5_CLIENT_KTNAME"]]
for path in required_files:
    if not os.path.exists(path):
        print(f"❌ Required file not found: {path}")
        exit(1)

print("Attempting to connect to Hive using PyHive...")

try:
    conn = hive.Connection(
        host=host,
        port=port,
        database=database,
        auth="KERBEROS",
        kerberos_service_name=kerberos_service_name
    )
    print("Connection established. Running test query...")
    curs = conn.cursor()
    curs.execute("SELECT current_database(), current_user()")
    result = curs.fetchall()
    print("✅ Connected:", result)
    curs.close()
    conn.close()
except Exception as e:
    print("❌ Connection failed:", e)
    traceback.print_exc()

input("Press Enter to exit...")

