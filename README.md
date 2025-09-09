# tap-zendesk
Tap for Zendesk

## Installation

1. Create and activate a virtualenv
1. `pip install -e '.[dev]'`

---

## Authentication

### Using OAuth

OAuth is the default authentication method for `tap-zendesk`. To use OAuth, you will need to fetch an `access_token` from a configured Zendesk integration. See https://support.zendesk.com/hc/en-us/articles/203663836 for more details on how to integrate your application with Zendesk.

**config.json**
```json
{
  "access_token": "AVERYLONGOAUTHTOKEN",
  "subdomain": "acme",
  "start_date": "2000-01-01T00:00:00Z",
  "request_timeout": 300
}
```
- `request_timeout` (integer, `300`): It is the time for which request should wait to get response. It is an optional parameter and default request_timeout is 300 seconds.

#### Getting an OAuth Access Token

A helper script is provided to obtain OAuth access tokens:

```bash
python scripts/get_oauth_token.py
```

**Prerequisites:**
1. Register your application in Zendesk Admin → Apps and integrations → APIs → Zendesk API
2. Configure your `config.json` with:
   ```json
   {
     "client_id": "YOUR_CLIENT_ID",
     "client_secret": "YOUR_CLIENT_SECRET", 
     "subdomain": "your-subdomain",
     "redirect_uri": "http://localhost:8080/callback",
     "start_date": "2000-01-01T00:00:00Z"
   }
   ```

The script will:
1. Open your browser to the Zendesk authorization page
2. Prompt you to copy the authorization code from the redirect URL
3. Exchange the code for an access token
4. Automatically update your `config.json` with the access token
### Using API Tokens

For a simplified, but less granular setup, you can use the API Token authentication which can be generated from the Zendesk Admin page. See https://support.zendesk.com/hc/en-us/articles/226022787-Generating-a-new-API-token- for more details about generating an API Token. You'll then be able to use the admins's `email` and the generated `api_token` to authenticate.

**config.json**
```json
{
  "email": "user@domain.com",
  "api_token": "THISISAVERYLONGTOKEN",
  "subdomain": "acme",
  "start_date": "2000-01-01T00:00:00Z",
  "request_timeout": 300
}
```
- `request_timeout` (integer, `300`):It is the time for which request should wait to get response. It is an optional parameter and default request_timeout is 300 seconds.

Copyright &copy; 2018 Stitch
