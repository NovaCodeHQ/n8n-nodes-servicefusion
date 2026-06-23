# n8n-nodes-servicefusion

Self-hosted n8n community node for working with ServiceFusion.

This package provides a single **ServiceFusion** node with resource/operation style actions backed by `@pmip/servicefusion-adapter`.

## Self-hosted only

This package is intended for **self-hosted n8n**.
It is **not configured for n8n Cloud verification** because it depends on the ServiceFusion adapter at runtime.

## Installation

### From a packed/published package

Install the package alongside your self-hosted n8n instance:

```bash
npm install n8n-nodes-servicefusion
```

Then restart n8n.

### Local development

In this package directory:

```bash
npm install
npm run build
npm run dev
```

## Credentials

Create a **ServiceFusion API** credential in n8n with:

- **Client ID**
- **Client Secret**
- **Base URL** (optional, defaults to `https://api.servicefusion.com/v1`)

The node uses the adapter's OAuth/token handling internally.

## Operations

### Customer

- Get All
- Get
- Create
- Update
- Delete
- Search

### Job

- Get All
- Get
- Create
- Update
- Delete
- Search
- Get All Paged
- Batch Sync

### Estimate

- Get All
- Get
- Create
- Update
- Convert To Job

### Invoice

- Get All
- Get
- Create
- Update
- Send

### Technician

- Get All
- Get
- Get Schedule
- Assign Job

### Webhook

- Get All
- Create
- Delete

## Compatibility

- Built as an n8n community node package
- Intended for modern self-hosted n8n versions using `@n8n/node-cli`
- Verified in this repo with:
  - TypeScript compile success
  - `n8n-node build` success
  - node export/runtime smoke checks

## Notes

- This package vendors a bundled copy of the ServiceFusion adapter into the published `dist/` output so it can stay self-hosted and still satisfy n8n's no-runtime-dependencies packaging rule.
- For local `n8n-node dev`, Node.js 22 LTS is recommended for the smoothest `isolated-vm` compatibility.

## Resources

- [n8n community nodes docs](https://docs.n8n.io/integrations/#community-nodes)
- [ServiceFusion Adapter repository](https://github.com/rashidazarang/servicefusion-adapter)

## Version history

### 0.1.0

Initial self-hosted release with ServiceFusion customer, job, estimate, invoice, technician, and webhook operations.
