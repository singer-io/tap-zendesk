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
  "start_date": "2000-01-01T00:00:00Z"
}
```

### Using API Tokens

For a simplified, but less granular setup, you can use the API Token authentication which can be generated from the Zendesk Admin page. See https://support.zendesk.com/hc/en-us/articles/226022787-Generating-a-new-API-token- for more details about generating an API Token. You'll then be able to use the admins's `email` and the generated `api_token` to authenticate.

**config.json**
```json
{
  "email": "user@domain.com",
  "api_token": "THISISAVERYLONGTOKEN",
  "subdomain": "acme",
  "start_date": "2000-01-01T00:00:00Z"
}
```

Copyright &copy; 2018 Stitch
