import type { ICredentialType, INodeProperties } from 'n8n-workflow';

export class ServiceFusionApi implements ICredentialType {
	name = 'serviceFusionApi';

	displayName = 'ServiceFusion API';

	icon = 'fa:building' as const;

	documentationUrl = 'https://github.com/rashidazarang/servicefusion-adapter';

	properties: INodeProperties[] = [
		{
			displayName: 'Client ID',
			name: 'clientId',
			type: 'string',
			default: '',
			required: true,
			description: 'The OAuth client ID for the ServiceFusion API',
		},
		{
			displayName: 'Client Secret',
			name: 'clientSecret',
			type: 'string',
			typeOptions: { password: true },
			default: '',
			required: true,
			description: 'The OAuth client secret for the ServiceFusion API',
		},
		{
			displayName: 'Base URL',
			name: 'baseUrl',
			type: 'string',
			default: 'https://api.servicefusion.com/v1',
			required: false,
			description: 'The base URL for the ServiceFusion API',
		},
	];

	// Credential testing is performed by the ServiceFusion node at execution time.
	// For self-hosted use, the adapter validates credentials via its own OAuth flow.
}
