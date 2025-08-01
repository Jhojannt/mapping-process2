# manual_setup.py - Ejecutar ANTES de usar Streamlit
"""
Script para crear manualmente las bases de datos para tus clientes existentes
Ejecuta este script UNA VEZ antes de usar la aplicación Streamlit
"""

import sys
import os
from Enhanced_MultiClient_Database import (
    create_enhanced_client_databases,
    test_client_database_connection,
    get_available_clients,
    verify_client_database_structure
)

def setup_client_databases():
    """Setup databases for existing clients"""
    
    print("🔧 Manual Database Setup")
    print("=" * 50)
    
    # 1. Test connection first
    print("\n1. Testing database connection...")
    success, message = test_client_database_connection()
    print(f"   {message}")
    
    if not success:
        print("❌ Cannot proceed without database connection!")
        print("   Check your database configuration:")
        print(f"   - Host: {os.getenv('DB_HOST', 'localhost')}")
        print(f"   - User: {os.getenv('DB_USER', 'root')}")
        print(f"   - Password: {'*' * len(os.getenv('DB_PASSWORD', 'Maracuya123'))}")
        return False
    
    # 2. Create databases for known clients
    clients_to_create = [
        "bill_doran",           # Tu cliente actual
        "demo_client",          # Cliente demo
        "test_company",         # Cliente test
        "sample_client"         # Cliente sample
    ]
    
    print(f"\n2. Creating databases for {len(clients_to_create)} clients...")
    
    for client_id in clients_to_create:
        print(f"\n   Creating databases for: {client_id}")
        try:
            success, message = create_enhanced_client_databases(client_id)
            if success:
                print(f"   ✅ {message}")
            else:
                print(f"   ❌ {message}")
                
            # Verify creation
            success, results = verify_client_database_structure(client_id)
            if success:
                print(f"   ✅ All databases verified for {client_id}")
            else:
                print(f"   ⚠️ Some issues found:")
                for db_type, status in results.items():
                    if "❌" in status:
                        print(f"      - {db_type}: {status}")
                        
        except Exception as e:
            print(f"   ❌ Error creating databases for {client_id}: {str(e)}")
    
    # 3. List all available clients
    print(f"\n3. Checking available clients...")
    available_clients = get_available_clients()
    if available_clients:
        print(f"   ✅ Found {len(available_clients)} clients:")
        for client in available_clients:
            print(f"      - {client}")
    else:
        print("   ⚠️ No clients found in database")
    
    print(f"\n4. Setup complete!")
    print("   You can now run: streamlit run streamlit_app.py")
    print("=" * 50)
    
    return True

if __name__ == "__main__":
    setup_client_databases()