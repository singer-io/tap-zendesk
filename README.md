# tap-zendesk
Tap for Zendesk

## Installation

1. Create and activate a virtualenv
1. `pip install -e '.[dev]'`

---

## Authentication

### Using OAuth

**config.json**
```json
{
  "access_token": "AVERYLONGOAUTHTOKEN",
  "subdomain": "acme",
  "start_date": "2000-01-01T00:00:00Z"
}
```

### Using API Tokens

Use the following configuration:

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
