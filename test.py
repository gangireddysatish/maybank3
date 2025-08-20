print("Script started", flush=True)
import os
import sys
import jaydebeapi
import jpype
import traceback
import logging

# === LOGGING SETUP ===
LOG_FILE = r"C:\SoftwareDontDelete\hive\dve_debug.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,
    filemode="w",
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Also log to console
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.DEBUG)
logging.getLogger().addHandler(console)

def debug_log(msg):
    print(msg, flush=True)  # always flush to console
    logging.debug(msg)

def main():
    debug_log("=== Starting Hive Connection Script ===")

    # JVM debug flags
    os.environ['JAVA_TOOL_OPTIONS'] = (
        "-Djavax.net.debug=ssl,handshake "
        "-Dsun.security.krb5.debug=true "
        "-Djava.security.debug=access,login,config,failure"
    )

    # Hardcoded paths
    JAR_PATHS = [r"C:\SoftwareDontDelete\hive\cdp-standalone.jar"]
    TRUSTSTORE_PATH = r"C:\SoftwareDontDelete\hive\cm-auto-global_truststore.jks"

    JDBC_URL = (
        "jdbc:hive2://mbbcdpdevapp001.maybank.com.my:10000/default;"
        "principal=hive/mbbcdpdevapp001.maybank.com.my@MBBUAT.LOCAL;"
        "auth=KERBEROS;"
        "ssl=true;"
        f"sslTrustStore={TRUSTSTORE_PATH};"
        "trustStorePassword=cfNvDNS6v0TiU72XGE24jSbxG4tLsQQWz1yEUBKDfgI;"
        "trustStoreType=jks;"
    )

    DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver"

    # Validate paths
    debug_log("🔎 Validating paths...")
    missing = False
    for jar in JAR_PATHS:
        if not os.path.exists(jar):
            debug_log(f"❌ JAR missing: {jar}")
            missing = True
        else:
            debug_log(f"✅ JAR found: {jar}")

    if not os.path.exists(TRUSTSTORE_PATH):
        debug_log(f"❌ Truststore missing: {TRUSTSTORE_PATH}")
        missing = True
    else:
        debug_log(f"✅ Truststore found: {TRUSTSTORE_PATH}")

    if missing:
        debug_log("❌ One or more required files missing. Exiting early.")
        return

    # Start JVM
    try:
        debug_log("🚀 Trying to start JVM...")
        if not jpype.isJVMStarted():
            jpype.startJVM(jpype.getDefaultJVMPath())
        debug_log("✅ JVM started successfully")
    except Exception as e:
        debug_log(f"❌ JVM failed to start: {e}")
        traceback.print_exc()
        return

        # Hive connection
    try:
        debug_log("🚀 Attempting to connect to Hive...")
        conn = jaydebeapi.connect(DRIVER_CLASS, JDBC_URL, [], JAR_PATHS)
        debug_log("✅ Connection established")

        curs = conn.cursor()
        curs.execute("SELECT 1")
        result = curs.fetchall()
        debug_log(f"✅ Query executed successfully: {result}")
        curs.close()
        conn.close()
        debug_log("✅ Connection closed cleanly")

    except Exception as e:
        debug_log("❌ Hive connection failed")
        print("Exception:", e)
        traceback.print_exc()
        # Extra guidance if it's a missing class issue
        if "ClassNotFoundException" in str(e) or "NoClassDefFoundError" in str(e):
            debug_log("💡 Looks like a dependency JAR is missing.")
            debug_log("   Try adding the full Cloudera Hive JDBC driver package (multiple jars).")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        debug_log("🔥 Top-level crash")
        debug_log(str(e))
        traceback.print_exc()
    finally:
        input("Press Enter to exit...")
