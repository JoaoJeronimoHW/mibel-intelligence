"""
Quick script to create .env file with API key.
Run once: python create_env.py
"""

api_key = input("Paste your ENTSO-E API key here: ")

with open('.env', 'w') as f:
    f.write(f'ENTSOE_API_KEY={api_key}\n')

print("✅ .env file created successfully!")
print(f"✅ API key saved (starts with: {api_key[:8]}...)")