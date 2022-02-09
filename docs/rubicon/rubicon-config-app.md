# Rubicon specific list of application configuration options

This document describes all configuration properties available for Prebid Server forked by Rubicon.

## GDPR
- `gdpr.rubicon.enable-cookie` - this setting enables cookie usage or in other words enables /cookie_sync or /setuid enpoints
- `gdpr.rubicon.rsid-cookie-encryption-key` - encryption key to decode RSID cookie value.
- `gdpr.rubicon.audit-cookie-encryption-key` - set key for uid audit cookie using Blowfish algorithm.
- `gdpr.rubicon.host-ip` - the ip pbs hosted on.

## Geo location
- `geolocation.netacuity.server` - comma separated list of the NetAcuity database server addresses.

## Analytics
- `analytics.rp.enabled` - if equals to `true` the analytics information will be submitted to Rubicon Analytics Service.
- `analytics.rp.host-url` - the protocol + host value to submit analytics (don't add `/event` at the end).
- `analytics.rp.pbs-version` - indicates Prebid Server version in analytics request.
- `analytics.rp.sampling-factor` - determines how many requests will be sent to Rubicon Analytics Service. If NULL or less or equals zero  - analytics will be sent only if account sampling factor configured.
- `analytics.rp.host-vendor-id` - determines vendor-id of analytic reporter provider.
