# @novacodehq/n8n-nodes-servicefusion

[![npm version](https://img.shields.io/npm/v/@novacodehq/n8n-nodes-servicefusion)](https://www.npmjs.com/package/@novacodehq/n8n-nodes-servicefusion)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Self-hosted n8n community node for [ServiceFusion](https://www.servicefusion.com) field service management. Provides a single **ServiceFusion** node backed by the ServiceFusion adapter, with full CRUD and search operations across 10 resources.

## Package

```text
@novacodehq/n8n-nodes-servicefusion
```

Published to npm under the `@novacodehq` scope. Built as an n8n community node package for **self-hosted n8n** instances only.

## Self-hosted only

This package is intended for **self-hosted n8n** only. It is not configured for n8n Cloud verification because it bundles the ServiceFusion adapter as a runtime dependency.

## Resources & operations

| Resource      | Operations                                                              |
| ------------- | ----------------------------------------------------------------------- |
| Customer      | Get All, Get, Create, Update, Delete, Search                            |
| Job           | Get All, Get, Create, Update, Delete, Search, Get All Paged, Batch Sync |
| Estimate      | Get All, Get, Create, Update, Convert To Job, Search                    |
| Invoice       | Get All, Get, Create, Update, Send                                      |
| Technician    | Get All, Get, Get Schedule, Assign Job                                  |
| Job Category  | Get All, Get                                                            |
| Job Status    | Get All, Get                                                            |
| Payment Type  | Get All, Get                                                            |
| Source        | Get All, Get                                                            |
| Calendar Task | Get All, Get                                                            |

### Highlights

- **Customer** supports expand parameters for contacts, custom fields, and locations.
- **Job** includes `Get All Paged` (cursor-based pagination) and `Batch Sync` (upsert via external ID, customer ID, or description).
- **Estimate** offers extensive search filters across status, dates, customer details, PO number, and more.
- **Invoice** supports status filtering, date range queries, and email delivery via `Send`.
- **Technician** covers schedule lookup and job assignment.
- **Job Category**, **Job Status**, **Payment Type**, **Source**, and **Calendar Task** are read-only list/lookup resources.

## Installation

### From npm

```bash
npm install @novacodehq/n8n-nodes-servicefusion
```

Then restart your self-hosted n8n instance.

### Local development

```bash
npm install
npm run build
npm run dev
```

## Credentials

Create a **ServiceFusion API** credential in n8n with:

| Field         | Required | Default                            |
| ------------- | -------- | ---------------------------------- |
| Client ID     | Yes      | —                                  |
| Client Secret | Yes      | —                                  |
| Base URL      | No       | `https://api.servicefusion.com/v1` |

The node uses the adapter's OAuth2 flow internally and validates credentials on first execution via a lightweight test call.

## Compatibility

| Requirement    | Notes                                                 |
| -------------- | ----------------------------------------------------- |
| n8n version    | Modern self-hosted n8n versions using `@n8n/node-cli` |
| Node.js        | 22 LTS recommended for `isolated-vm` compatibility    |
| Package format | `@n8n/node-cli` community node package                |
| n8n cloud      | Not supported — self-hosted only                      |

## Error handling

Errors from the ServiceFusion API are enriched with:

- **HTTP status code** from the failed request
- **Request details** (method and URL) for debugging
- **Response body** from the adapter's debug state
- **Original error message** preserved in the description

The node respects n8n's `continueOnFail` — errors on individual items produce error JSON outputs rather than failing the entire execution when enabled.

## Notes

- This package vendors a bundled copy of the ServiceFusion adapter (`nodes/ServiceFusion/vendor/servicefusion-adapter.bundle.js`) at build time. This satisfies n8n's community node packaging rules while keeping the adapter as a runtime dependency.
- The adapter is bundled via esbuild from the `@rashidazarang/servicefusion-adapter` npm package (aliased as `@pmip/servicefusion-adapter` in `package.json`).
- For local development with `n8n-node dev`, Node.js 22 LTS is recommended for smoothest `isolated-vm` compatibility.

## Resources

- [n8n community nodes documentation](https://docs.n8n.io/integrations/#community-nodes)
- [ServiceFusion API documentation](https://developer.servicefusion.com)
- [ServiceFusion adapter repository](https://github.com/rashidazarang/servicefusion-adapter)

## Version history

See [CHANGELOG.md](CHANGELOG.md).

## License

MIT © [NovaCodeHQ](https://github.com/NovaCodeHQ)
