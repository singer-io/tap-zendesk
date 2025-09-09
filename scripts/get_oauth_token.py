#!/usr/bin/env python3
"""
Helper script to get Zendesk OAuth access token using client credentials flow.
"""

import json
import requests
import sys
import os
from urllib.parse import urlencode, urlparse, parse_qs
import webbrowser

def load_config():
    """Load configuration from config.json in the parent directory"""
    # Look for config.json in the parent directory (project root)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, '..', 'config.json')
    
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: config.json not found at {config_path}")
        print("Please ensure config.json exists in the project root directory")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {config_path}")
        sys.exit(1)

def get_access_token():
    """
    Get Zendesk OAuth access token using authorization code flow.
    This requires manual intervention to complete the OAuth flow.
    """
    config = load_config()
    
    client_secret = config.get('client_secret')
    subdomain = config.get('subdomain')
    redirect_uri = config.get('redirect_uri')
    
    
    if not client_secret or not subdomain:
        print("Error: client_secret and subdomain are required in config.json")
        sys.exit(1)
    
    # For Zendesk OAuth, you need to register your app and get client_id
    # This is a placeholder - you'll need to replace with your actual client_id
    print("To get an access token, you need to:")
    print("1. Register your app in Zendesk Admin -> Apps and integrations -> APIs -> Zendesk API")
    print("2. Get your client_id from the app registration")
    print("3. Update this script with your client_id")
    print()
    
    # Check if client_id is in config first
    client_id = config.get('client_id')
    if not client_id:
        print("Error: client_id not found in config.json")
        print("Please add your client_id to config.json or run this script interactively")
        print("You can get your client_id from Zendesk Admin -> Apps and integrations -> APIs -> Zendesk API")
        sys.exit(1)
    
    
    # Step 1: Get authorization code
    auth_url = f"https://{subdomain}.zendesk.com/oauth/authorizations/new"
    
    params = {
        'response_type': 'code',
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'scope': 'read write'
    }
    
    authorization_url = f"{auth_url}?{urlencode(params)}"
    
    print(f"1. Visit this URL to authorize the application:")
    print(f"   {authorization_url}")
    print()
    print("2. After authorization, you'll be redirected to localhost:8080/callback?code=...")
    print("3. Copy the 'code' parameter from the redirect URL")
    
    # Try to open in browser automatically
    try:
        webbrowser.open(authorization_url)
        print("\nOpening authorization URL in your default browser...")
    except:
        print("\nCould not open browser automatically. Please copy the URL above.")
    
    print()
    authorization_code = input("Enter the authorization code from the redirect URL: ").strip()
    
    if not authorization_code:
        print("Error: authorization code is required")
        sys.exit(1)
    
    # Step 2: Exchange authorization code for access token
    token_url = f"https://{subdomain}.zendesk.com/oauth/tokens"
    
    token_data = {
        'grant_type': 'authorization_code',
        'code': authorization_code,
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': redirect_uri
    }
    
    print("Exchanging authorization code for access token...")
    
    try:
        response = requests.post(token_url, data=token_data)
        response.raise_for_status()
        
        token_response = response.json()
        access_token = token_response.get('access_token')
        
        if access_token:
            print(f"\nSuccess! Your access token is:")
            print(f"{access_token}")
            
            # Update config.json with the new access token
            config['access_token'] = access_token
            script_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(script_dir, '..', 'config.json')
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
            
            print(f"\nconfig.json has been updated with your access token.")
            
            # Also show refresh token if available
            refresh_token = token_response.get('refresh_token')
            if refresh_token:
                print(f"Refresh token: {refresh_token}")
                print("(You can use this to get new access tokens when the current one expires)")
        else:
            print("Error: No access token in response")
            print(f"Response: {token_response}")
            
    except requests.exceptions.RequestException as e:
        print(f"Error making token request: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_detail = e.response.json()
                print(f"Error details: {error_detail}")
            except:
                print(f"Response text: {e.response.text}")
        sys.exit(1)

if __name__ == "__main__":
    get_access_token()