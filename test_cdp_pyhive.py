from pyhive import hive
import os
import traceback
import subprocess
import sys

# pip install thrift
# pip install pyhive[hive]
# pip install sasl
# pip install thrift_sasl
# pip install kerberos
# pip install pykerberos
# pip install requests-kerberos
# check principal, keytab_path, 


def check_kerberos_ticket():
    """Check if valid Kerberos ticket exists"""
    try:
        result = subprocess.run(['klist'], capture_output=True, text=True)
        if result.returncode != 0:
            print("❌ No valid Kerberos ticket found.")
            return False
        else:
            print("✅ Valid Kerberos ticket found:")
            print(result.stdout)
            return True
    except FileNotFoundError:
        print("⚠️  klist command not found. Cannot check Kerberos ticket status.")
        return None

def authenticate_with_keytab():
    """Authenticate using keytab file"""
    keytab_path = r"C:\ProgramData\MIT\Kerberos5\80013010-cdp.keytab"
    principal = "hive/mbbcdpdevapp001.maybank.com.my@MBBUAT.LOCAL"  # Replace with your actual principal
    
    try:
        print(f"Attempting to authenticate with keytab: {keytab_path}")
        result = subprocess.run([
            'kinit', 
            principal, 
            '-kt', 
            keytab_path
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Kerberos authentication successful")
            return True
        else:
            print(f"❌ Kerberos authentication failed: {result.stderr}")
            return False
    except FileNotFoundError:
        print("❌ kinit command not found. Please install Kerberos client tools.")
        return False

def main():
    # Set Kerberos environment variables
    keytab_path = r"C:\ProgramData\MIT\Kerberos5\80013010-cdp.keytab"
    os.environ["KRB5_CLIENT_KTNAME"] = keytab_path
    
    print(f"Keytab path: {keytab_path}")
    
    # Check keytab file existence
    if not os.path.exists(keytab_path):
        print(f"❌ Required keytab file not found: {keytab_path}")
        sys.exit(1)
    
    print("✅ Keytab file found")
    
    # Check for existing Kerberos ticket
    ticket_status = check_kerberos_ticket()
    
    # If no valid ticket, try to authenticate with keytab
    if ticket_status is False:
        print("Attempting to authenticate with keytab...")
        if not authenticate_with_keytab():
            print("❌ Failed to authenticate with keytab. Exiting.")
            sys.exit(1)
    
    print("Attempting to connect to Hive using PyHive...")
    
    try:
        # Connection with SSL and HTTP transport mode
        conn = hive.Connection(
            host='mbbcdpdevapp001.maybank.com.my',
            port=10000,
            database='default',
            auth='KERBEROS',
            kerberos_service_name='hive',
            configuration={
                'hive.server2.transport.mode': 'http',
                'hive.server2.thrift.http.path': 'cliservice',
                'hive.server2.use.SSL': 'true'
            }
        )
        
        print("✅ Connection established successfully!")
        print("Running test query...")
        
        # Test the connection
        cursor = conn.cursor()
        cursor.execute("SELECT current_database(), current_user()")
        result = cursor.fetchall()
        
        print("✅ Query executed successfully:")
        print(f"Current database: {result[0][0]}")
        print(f"Current user: {result[0][1]}")
        
        # Additional test queries
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        print(f"✅ Available databases: {[db[0] for db in databases[:5]]}")  # Show first 5
        
        cursor.close()
        conn.close()
        print("✅ Connection closed successfully")
        
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        print("\nFull error traceback:")
        traceback.print_exc()
        
        # Try alternative connection without SSL/HTTP transport
        print("\n🔄 Trying alternative connection without SSL...")
        try:
            conn_alt = hive.Connection(
                host='mbbcdpdevapp001.maybank.com.my',
                port=10000,
                database='default',
                auth='KERBEROS',
                kerberos_service_name='hive'
            )
            
            cursor_alt = conn_alt.cursor()
            cursor_alt.execute("SELECT current_database()")
            result_alt = cursor_alt.fetchall()
            print(f"✅ Alternative connection successful: {result_alt}")
            
            cursor_alt.close()
            conn_alt.close()
            
        except Exception as e2:
            print(f"❌ Alternative connection also failed: {str(e2)}")
            
    finally:
        input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()

