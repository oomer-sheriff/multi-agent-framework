import os
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("GEMINI_API_KEY") # Ensure this is loaded from your .env
url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"

response = requests.get(url)
models = response.json().get("models", [])
print(response)
for model in models:
    print(f"Name: {model['name']}")
    print(f"Supported Methods: {model['supportedGenerationMethods']}")
    print("-" * 40)
